//! The Fluvio Producer module allows applications to send messages to topics in the Fluvio cluster.
//!
//! # Overview
//!
//! This module provides the necessary structures and functions to produce messages to a Fluvio topic.
//! It includes the `TopicProducerPool` struct, which manages the production of messages
//! to specific topics and partitions, respectively.
//!
use std::collections::HashMap;
use std::sync::Arc;

use tracing::instrument;
use async_lock::RwLock;
use anyhow::Result;

use fluvio_protocol::record::ReplicaKey;
use fluvio_protocol::record::Record;
use fluvio_compression::Compression;
#[cfg(feature = "compress")]
use fluvio_sc_schema::topic::CompressionAlgorithm;
use fluvio_types::PartitionId;
use fluvio_types::event::StickyEvent;

mod accumulator;
mod config;
mod error;
mod output;
mod record;
mod partitioning;
mod partition_producer;
mod memory_batch;

pub mod event;

pub use fluvio_protocol::record::{RecordKey, RecordData};

use crate::spu::SpuPool;
use crate::spu::SpuSocketPool;
use crate::FluvioError;
use crate::metrics::ClientMetrics;
use crate::producer::accumulator::{RecordAccumulator, PushRecord};

pub use crate::producer::partitioning::{Partitioner, PartitionerConfig};

use self::accumulator::BatchEvents;
use self::accumulator::BatchHandler;
use self::accumulator::BatchesDeque;
pub use self::config::{
    TopicProducerConfigBuilder, TopicProducerConfig, TopicProducerConfigBuilderError,
    DeliverySemantic, RetryPolicy, RetryStrategy,
};
pub use self::error::ProducerError;
use self::event::EventHandler;
pub use self::output::ProduceOutput;
use self::partition_producer::PartitionProducer;
pub use self::record::{FutureRecordMetadata, RecordMetadata};

/// Pool of producers for a given topic. There is a producer per partition
pub type TopicProducerPool = TopicProducer<SpuSocketPool>;

/// Pool of producers for a given topic. There is a producer per partition
struct ProducerPool {
    flush_events: HashMap<PartitionId, (Arc<EventHandler>, Arc<EventHandler>)>,
    end_events: HashMap<PartitionId, Arc<StickyEvent>>,
    errors: HashMap<PartitionId, Arc<RwLock<Option<ProducerError>>>>,
}

#[derive(Clone)]
struct PartitionProducerParams<S>
where
    S: SpuPool + Send + Sync + 'static,
{
    config: Arc<TopicProducerConfig>,
    spu_pool: Arc<S>,
    batches_deque: Arc<BatchesDeque>,
    batch_events: Arc<BatchEvents>,
    client_metric: Arc<ClientMetrics>,
}

impl ProducerPool {
    fn new<S>(
        config: Arc<TopicProducerConfig>,
        topic: String,
        spu_pool: Arc<S>,
        batches: Arc<HashMap<PartitionId, BatchHandler>>,
        client_metric: Arc<ClientMetrics>,
    ) -> Self
    where
        S: SpuPool + Send + Sync + 'static,
    {
        let mut end_events = HashMap::new();
        let mut flush_events = HashMap::new();
        let mut errors = HashMap::new();
        for (partition_id, (batch_events, batch_list)) in batches.iter() {
            let end_event = StickyEvent::shared();
            let flush_event = (EventHandler::shared(), EventHandler::shared());
            let replica = ReplicaKey::new(topic.clone(), *partition_id);
            let error = Arc::new(RwLock::new(None));

            let params = PartitionProducerParams {
                config: config.clone(),
                spu_pool: spu_pool.clone(),
                batches_deque: batch_list.clone(),
                batch_events: batch_events.clone(),
                client_metric: client_metric.clone(),
            };

            PartitionProducer::start(
                params,
                error.clone(),
                end_event.clone(),
                flush_event.clone(),
                replica,
            );
            errors.insert(*partition_id, error);
            end_events.insert(*partition_id, end_event);
            flush_events.insert(*partition_id, flush_event);
        }
        Self {
            end_events,
            flush_events,
            errors,
        }
    }

    async fn ensure_partition_producer<S>(
        &mut self,
        params: PartitionProducerParams<S>,
        topic: String,
        partition_id: PartitionId,
        record_accumulator: Arc<RecordAccumulator>,
    ) where
        S: SpuPool + Send + Sync + 'static,
    {
        if self.flush_events.contains_key(&partition_id) {
            return;
        }
        record_accumulator
            .add_partition(
                partition_id,
                (params.batch_events.clone(), params.batches_deque.clone()),
            )
            .await;

        let end_event = StickyEvent::shared();
        let flush_event = (EventHandler::shared(), EventHandler::shared());
        let replica = ReplicaKey::new(topic.clone(), partition_id);
        let error: Arc<RwLock<Option<ProducerError>>> = Arc::new(RwLock::new(None));

        PartitionProducer::start(
            params,
            error.clone(),
            end_event.clone(),
            flush_event.clone(),
            replica,
        );
        self.errors.insert(partition_id, error);
        self.end_events.insert(partition_id, end_event);
        self.flush_events.insert(partition_id, flush_event);
    }

    async fn flush_all_batches(&self) -> Result<()> {
        for ((_, (manual_flush_notifier, batch_flushed_event)), (_, error)) in
            self.flush_events.iter().zip(self.errors.iter())
        {
            let listener = batch_flushed_event.listen();
            manual_flush_notifier.notify().await;
            listener.await;
            {
                let error_handle = error.read().await;
                if let Some(error) = &*error_handle {
                    return Err(error.clone().into());
                }
            }
        }

        Ok(())
    }

    async fn last_error(&self, partition_id: PartitionId) -> Option<ProducerError> {
        let error = self.errors.get(&partition_id)?.read().await;
        error.clone()
    }

    async fn clear_errors(&self) {
        for (_, error) in self.errors.iter() {
            let mut error_handle = error.write().await;
            *error_handle = None;
        }
    }

    fn end(&self) {
        self.end_events.iter().for_each(|(_, event)| {
            event.notify();
        });
    }
}

impl Drop for ProducerPool {
    fn drop(&mut self) {
        self.end();
    }
}

/// An interface for producing events to a particular topic
///
/// A `TopicProducer` allows you to send events to the specific
/// topic it was initialized for. Once you have a `TopicProducer`,
/// you can send events to the topic, choosing which partition /// each event should be delivered to.
#[derive(Clone)]
pub struct TopicProducer<S>
where
    S: SpuPool + Send + Sync + 'static,
{
    inner: Arc<InnerTopicProducer<S>>,
    #[cfg(feature = "smartengine")]
    sm_chain: Option<Arc<RwLock<fluvio_smartengine::SmartModuleChainInstance>>>,
    #[allow(unused)]
    metrics: Arc<ClientMetrics>,
}

struct InnerTopicProducer<S>
where
    S: SpuPool + Send + Sync + 'static,
{
    config: Arc<TopicProducerConfig>,
    topic: String,
    spu_pool: Arc<S>,
    record_accumulator: Arc<RecordAccumulator>,
    producer_pool: Arc<RwLock<ProducerPool>>,
    metrics: Arc<ClientMetrics>,
}

impl<S> InnerTopicProducer<S>
where
    S: SpuPool + Send + Sync + 'static,
{
    /// Flush all the PartitionProducers and wait for them.
    async fn flush(&self) -> Result<()> {
        self.producer_pool.read().await.flush_all_batches().await?;
        Ok(())
    }

    async fn push_record(self: Arc<Self>, record: Record) -> Result<PushRecord> {
        let topics = self.spu_pool.topics();

        let topic_spec = topics
            .lookup_by_key(&self.topic)
            .await?
            .ok_or_else(|| FluvioError::TopicNotFound(self.topic.to_string()))?
            .spec;
        let partition_count = topic_spec.partitions();
        let partition_config = PartitionerConfig { partition_count };

        let key = record.key.as_ref().map(|k| k.as_ref());
        let value = record.value.as_ref();
        let partition = self
            .config
            .partitioner
            .partition(&partition_config, key, value);

        let mut producer_pool = self.producer_pool.write().await;

        if let Some(error) = producer_pool.last_error(partition).await {
            return Err(error.into());
        }

        let params = PartitionProducerParams {
            config: self.config.clone(),
            spu_pool: self.spu_pool.clone(),
            batches_deque: BatchesDeque::shared(),
            batch_events: BatchEvents::shared(),
            client_metric: self.metrics.clone(),
        };

        let _ = producer_pool
            .ensure_partition_producer(
                params,
                self.topic.clone(),
                partition,
                self.record_accumulator.clone(),
            )
            .await;

        let push_record = self
            .record_accumulator
            .push_record(record, partition)
            .await?;

        Ok(push_record)
    }

    async fn clear_errors(&self) {
        self.producer_pool.read().await.clear_errors().await;
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "smartengine")] {

        use std::collections::BTreeMap;
        use once_cell::sync::Lazy;

        use fluvio_spu_schema::server::smartmodule::SmartModuleContextData;
        use fluvio_smartengine::SmartEngine;

        pub use fluvio_smartengine::{SmartModuleChainBuilder, SmartModuleConfig, SmartModuleInitialData};

        static SM_ENGINE: Lazy<SmartEngine> = Lazy::new(|| {
            fluvio_smartengine::SmartEngine::new()
        });

        impl<S> TopicProducer<S>
            where
                S: SpuPool + Send + Sync + 'static,
        {
            /// Adds a chain of SmartModules to this TopicProducer
            pub async fn with_chain(mut self, chain_builder: SmartModuleChainBuilder) -> Result<Self> {
                let mut chain_instance = chain_builder.initialize(&SM_ENGINE).map_err(|e| FluvioError::Other(format!("SmartEngine - {e:?}")))?;
                chain_instance.look_back(|_| async { anyhow::bail!("lookback is not supported on engine running on Producer") }, &Default::default()).await?;
                self.sm_chain = Some(Arc::new(RwLock::new(chain_instance)));
                Ok(self)
            }

            /// Adds a SmartModule filter to this TopicProducer
            pub async fn with_filter(
                self,
                filter: impl  Into<Vec<u8>>,
                params: BTreeMap<String, String>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder().params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, filter))).await
            }

            /// Adds a SmartModule FilterMap to this TopicProducer
            pub async fn with_filter_map(
                self,
                map: impl Into<Vec<u8>>,
                params: BTreeMap<String, String>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder().params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, map))).await
            }

            /// Adds a SmartModule map to this TopicProducer
            pub async fn with_map(
                self,
                map: impl Into<Vec<u8>>,
                params: BTreeMap<String, String>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder().params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, map))).await
            }

            /// Adds a SmartModule array_map to this TopicProducer
            pub async fn with_array_map(
                self,
                map: impl Into<Vec<u8>>,
                params: BTreeMap<String, String>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder().params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, map))).await
            }

            /// Adds a SmartModule aggregate to this TopicProducer
            pub async fn with_aggregate(
                self,
                map: impl Into<Vec<u8>>,
                params: BTreeMap<String, String>,
                accumulator: Vec<u8>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder()
                    .initial_data(SmartModuleInitialData::Aggregate{accumulator})
                    .params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, map))).await
            }

            /// Use generic smartmodule (the type is detected in smartengine)
            pub async fn with_smartmodule(
                self,
                smartmodule: impl Into<Vec<u8>>,
                params: BTreeMap<String, String>,
                context: SmartModuleContextData,
            ) -> Result<Self> {
                let mut config_builder = SmartModuleConfig::builder();
                config_builder.params(params.into());
                if let SmartModuleContextData::Aggregate{accumulator} = context {
                    config_builder.initial_data(SmartModuleInitialData::Aggregate{accumulator});
                };
                self.with_chain(SmartModuleChainBuilder::from((config_builder.build()?, smartmodule))).await
            }

        }
    }
}

impl<S> TopicProducer<S>
where
    S: SpuPool + Send + Sync + 'static,
{
    pub(crate) async fn new(
        topic: String,
        spu_pool: Arc<S>,
        config: Arc<TopicProducerConfig>,
        metrics: Arc<ClientMetrics>,
    ) -> Result<Self> {
        let topics = spu_pool.topics();
        let topic_spec: fluvio_sc_schema::topic::TopicSpec = topics
            .lookup_by_key(&topic)
            .await?
            .ok_or_else(|| FluvioError::TopicNotFound(topic.to_string()))?
            .spec;
        let partition_count = topic_spec.partitions();

        cfg_if::cfg_if! {
            if #[cfg(feature = "compress")] {
                let compression = determine_producer_compression_algo(config.clone(), topic_spec)?;
            } else {
                let compression = Compression::None;
            }
        }

        let record_accumulator = RecordAccumulator::new(
            config.batch_size,
            config.max_request_size,
            config.batch_queue_size,
            partition_count,
            compression,
        );
        let producer_pool = ProducerPool::new(
            config.clone(),
            topic.clone(),
            spu_pool.clone(),
            Arc::new(record_accumulator.batches().await),
            metrics.clone(),
        );

        Ok(Self {
            inner: Arc::new(InnerTopicProducer {
                config,
                topic,
                spu_pool,
                producer_pool: Arc::new(RwLock::new(producer_pool)),
                record_accumulator: Arc::new(record_accumulator),
                metrics: metrics.clone(),
            }),
            #[cfg(feature = "smartengine")]
            sm_chain: Default::default(),
            metrics,
        })
    }

    pub fn topic(&self) -> &str {
        &self.inner.topic
    }

    pub fn config(&self) -> &TopicProducerConfig {
        &self.inner.config
    }

    /// Send all the queued records in the producer batches.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{TopicProducerPool, FluvioError};
    /// # async fn example(producer: &TopicProducerPool) -> anyhow::Result<()> {
    /// producer.send("Key", "Value").await?;
    /// producer.flush().await?;
    /// # Ok(())
    /// # }
    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    /// Sends a key/value record to this producer's Topic.
    ///
    /// The partition that the record will be sent to is derived from the Key.
    ///
    ///  Depending on the producer configuration, a `send` call will not send immediately
    ///  the record to the SPU. Instead, it could add the record to a batch.
    ///  `TopicProducer::flush` is used to immediately send all the queued records in the producer batches.
    ///
    /// If the batch queue is full, a `send` call will block until there will be enough space for new batch.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{TopicProducerPool, FluvioError};
    /// # async fn example(producer: &TopicProducerPool) -> anyhow::Result<()> {
    /// producer.send("Key", "Value").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(
        skip(self, key, value),
        fields(topic = %self.inner.topic),
    )]
    pub async fn send(
        &self,
        key: impl Into<RecordKey>,
        value: impl Into<RecordData>,
    ) -> Result<ProduceOutput> {
        let record_key = key.into();
        let record_value = value.into();
        let record = Record::from((record_key, record_value));

        cfg_if::cfg_if! {
            if #[cfg(feature = "smartengine")] {
                let mut entries = vec![record];

                use chrono::Utc;

                use fluvio_smartengine::DEFAULT_SMARTENGINE_VERSION;
                use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;


                let metrics = self.metrics.chain_metrics();

                if let Some(
                    smart_chain_ref
                ) = &self.sm_chain {
                    let mut sm_chain = smart_chain_ref.write().await;
                    let mut sm_input = SmartModuleInput::try_from_records(entries, DEFAULT_SMARTENGINE_VERSION)?;
                    let current_time = Utc::now().timestamp_millis();

                    sm_input.set_base_timestamp(current_time);

                    let output = sm_chain.process(sm_input,metrics).map_err(|e| FluvioError::Other(format!("SmartEngine - {e:?}")))?;
                    entries = output.successes;
                }
            } else {
                let  entries = vec![record];
                }
        }

        let mut results = ProduceOutput::default();
        for record in entries {
            let push_record = self.inner.clone().push_record(record).await?;
            results.add(push_record.future);
        }
        Ok(results)
    }

    #[instrument(
        skip(self, records),
        fields(topic = %self.inner.topic),
    )]
    pub async fn send_all(
        &self,
        records: impl IntoIterator<Item = (impl Into<RecordKey>, impl Into<RecordData>)>,
    ) -> Result<Vec<ProduceOutput>> {
        let mut results = vec![];
        for (key, value) in records {
            let produce_output = self.send(key, value).await?;
            results.push(produce_output);
        }

        Ok(results)
    }

    /// Clear partition producers errors in order to make partition producers available.
    /// This is needed once an error is present in order to send new records again.
    pub async fn clear_errors(&self) {
        self.inner.clear_errors().await;
    }

    /// Return a shared instance of `ClientMetrics`
    pub fn metrics(&self) -> Arc<ClientMetrics> {
        self.metrics.clone()
    }
}

#[cfg(feature = "compress")]
fn determine_producer_compression_algo(
    config: Arc<TopicProducerConfig>,
    topic_spec: fluvio_sc_schema::topic::TopicSpec,
) -> Result<Compression> {
    let result = match topic_spec.get_compression_type() {
        CompressionAlgorithm::Any => config.compression.unwrap_or_default(),
        CompressionAlgorithm::Gzip => match config.compression {
            Some(Compression::Gzip) | None => Compression::Gzip,
            Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                format!("Compression in the producer ({compression_config}) does not match with topic level compression (gzip)" ),
            )).into()),
        },
        CompressionAlgorithm::Snappy => match config.compression {
            Some(Compression::Snappy) | None => Compression::Snappy,
            Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                format!("Compression in the producer ({compression_config}) does not match with topic level compression (snappy)" ),
            )).into()),
        },
        CompressionAlgorithm::Lz4 => match config.compression {
            Some(Compression::Lz4) | None => Compression::Lz4,
            Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                format!("Compression in the producer ({compression_config}) does not match with topic level compression (lz4)"),
            )).into()),
        },
        CompressionAlgorithm::Zstd => match config.compression {
            Some(Compression::Zstd) | None => Compression::Zstd,
            Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                format!("Compression in the producer ({compression_config}) does not match with topic level compression (zstd)" ),
            )).into()),
        },
    CompressionAlgorithm::None => match config.compression {
            Some(Compression::None) | None => Compression::None,
            Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                format!("Compression in the producer ({compression_config}) does not match with topic level compression (no compression)" )

            )).into()),
        },
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use fluvio_protocol::record::RecordKey;
    use fluvio_sc_schema::{partition::PartitionSpec, store::MetadataStoreObject, topic::TopicSpec};
    use fluvio_socket::{ClientConfig, SocketError, StreamSocket, VersionedSerialSocket};
    use fluvio_stream_dispatcher::metadata::local::LocalMetadataItem;
    use fluvio_types::SpuId;

    use crate::{
        metrics::ClientMetrics,
        spu::SpuPool,
        sync::{MetadataStores, StoreContext},
        FluvioError, TopicProducer, TopicProducerConfig,
    };

    struct SpuPoolMock {
        topics: StoreContext<TopicSpec>,
    }

    #[async_trait]
    impl SpuPool for SpuPoolMock {
        fn start(
            _config: Arc<ClientConfig>,
            _metadata: MetadataStores,
        ) -> Result<Self, SocketError> {
            todo!()
        }

        async fn connect_to_leader(&self, _leader: SpuId) -> Result<StreamSocket, FluvioError> {
            todo!()
        }

        async fn create_serial_socket_from_leader(
            &self,
            _leader_id: SpuId,
        ) -> Result<VersionedSerialSocket, FluvioError> {
            todo!()
        }

        async fn topic_exists(&self, _topic: String) -> Result<bool, FluvioError> {
            todo!()
        }

        fn shutdown(&mut self) {
            todo!()
        }

        fn topics(&self) -> &StoreContext<TopicSpec> {
            &self.topics
        }

        fn partitions(&self) -> &StoreContext<PartitionSpec> {
            todo!()
        }
    }

    #[fluvio_future::test]
    async fn test_topic_producer_should_detect_new_partitions() {
        let topic = "test".to_string();
        let config = Arc::new(TopicProducerConfig::default());
        let metrics = Arc::new(ClientMetrics::default());
        let partitions_count = 2;

        let topic_2_partitions = vec![
            MetadataStoreObject::<TopicSpec, LocalMetadataItem>::with_spec(
                "test",
                (partitions_count, 2, false).into(), // 2 partitions, 2 replicas, not ignore rack
            ),
        ];

        let topics = StoreContext::<TopicSpec>::new();
        let spu_pool = Arc::new(SpuPoolMock { topics });
        spu_pool.topics().store().sync_all(topic_2_partitions).await;
        let producer = TopicProducer::new(topic.clone(), spu_pool.clone(), config, metrics)
            .await
            .expect("producer");

        let _ = producer
            .send(RecordKey::NULL, "123".to_string())
            .await
            .expect("send");
        let _ = producer
            .send(RecordKey::NULL, "456".to_string())
            .await
            .expect("send");

        let batches = producer.inner.record_accumulator.batches().await;

        assert_eq!(batches.len(), partitions_count as usize);

        let producer_pool = producer.inner.producer_pool.read().await;
        assert_eq!(producer_pool.errors.len(), partitions_count as usize);
        assert!(producer_pool.errors.get(&0).unwrap().read().await.is_none());
        assert!(producer_pool.errors.get(&1).unwrap().read().await.is_none());
        assert_eq!(producer_pool.flush_events.len(), partitions_count as usize);
        assert_eq!(producer_pool.end_events.len(), partitions_count as usize);
        drop(producer_pool);

        assert_eq!(producer.inner.topic, topic.clone());

        let new_partitions_count = 3;

        let topic_3_partitions = vec![
            MetadataStoreObject::<TopicSpec, LocalMetadataItem>::with_spec(
                "test",
                (new_partitions_count, 2, false).into(), // 3 partitions, 2 replicas, not ignore rack
            ),
        ];

        spu_pool.topics.store().sync_all(topic_3_partitions).await;

        let _ = producer
            .send(RecordKey::NULL, "789".to_string())
            .await
            .expect("send");

        let producer_pool = producer.inner.producer_pool.read().await;
        assert_eq!(producer_pool.errors.len(), new_partitions_count as usize);
        assert!(producer_pool.errors.get(&0).unwrap().read().await.is_none());
        assert!(producer_pool.errors.get(&1).unwrap().read().await.is_none());
        assert_eq!(
            producer_pool.flush_events.len(),
            new_partitions_count as usize
        );
        assert_eq!(
            producer_pool.end_events.len(),
            new_partitions_count as usize
        );
        drop(producer_pool);
    }
}
