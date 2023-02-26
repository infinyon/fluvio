use std::sync::Arc;

use tracing::instrument;
use async_lock::RwLock;
use anyhow::Result;

use fluvio_protocol::record::ReplicaKey;
use fluvio_protocol::record::Record;
use fluvio_compression::Compression;
use fluvio_sc_schema::topic::CompressionAlgorithm;
use fluvio_types::PartitionId;
use fluvio_types::event::StickyEvent;

mod accumulator;
mod config;
mod error;
pub mod event;
mod output;
mod record;
mod partitioning;
mod partition_producer;
mod memory_batch;

pub use fluvio_protocol::record::{RecordKey, RecordData};

use crate::FluvioError;
use crate::metrics::ClientMetrics;
use crate::spu::SpuPool;
use crate::producer::accumulator::{RecordAccumulator, PushRecord};
pub use crate::producer::partitioning::{Partitioner, PartitionerConfig};
#[cfg(feature = "stats")]
use crate::stats::{ClientStats, ClientStatsDataCollect, metrics::ClientStatsDataFrame};

use self::accumulator::{BatchHandler};
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
struct ProducerPool {
    flush_events: Vec<(Arc<EventHandler>, Arc<EventHandler>)>,
    end_events: Vec<Arc<StickyEvent>>,
    errors: Vec<Arc<RwLock<Option<ProducerError>>>>,
}

impl ProducerPool {
    fn new(
        config: Arc<TopicProducerConfig>,
        topic: String,
        spu_pool: Arc<SpuPool>,
        batches: Arc<Vec<BatchHandler>>,
        client_metric: Arc<ClientMetrics>,
    ) -> Self {
        let mut end_events = vec![];
        let mut flush_events = vec![];
        let mut errors = vec![];
        for (partition_id, (batch_events, batch_list)) in batches.iter().enumerate() {
            let end_event = StickyEvent::shared();
            let flush_event = (EventHandler::shared(), EventHandler::shared());
            let replica = ReplicaKey::new(topic.clone(), partition_id as PartitionId);
            let error = Arc::new(RwLock::new(None));

            PartitionProducer::start(
                config.clone(),
                replica,
                spu_pool.clone(),
                batch_list.clone(),
                batch_events.clone(),
                error.clone(),
                end_event.clone(),
                flush_event.clone(),
                client_metric.clone(),
            );
            errors.push(error);
            end_events.push(end_event);
            flush_events.push(flush_event);
        }
        Self {
            end_events,
            flush_events,
            errors,
        }
    }

    fn shared(
        config: Arc<TopicProducerConfig>,
        topic: String,
        spu_pool: Arc<SpuPool>,
        batches: Arc<Vec<BatchHandler>>,
        client_metric: Arc<ClientMetrics>,
    ) -> Arc<Self> {
        Arc::new(ProducerPool::new(
            config,
            topic,
            spu_pool,
            batches,
            client_metric,
        ))
    }

    async fn flush_all_batches(&self) -> Result<()> {
        for ((manual_flush_notifier, batch_flushed_event), error) in
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
        let error = self.errors[partition_id as usize].read().await;
        error.clone()
    }

    async fn clear_errors(&self) {
        for error in self.errors.iter() {
            let mut error_handle = error.write().await;
            *error_handle = None;
        }
    }

    fn end(&self) {
        for event in &self.end_events {
            event.notify();
        }
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
/// you can send events to the topic, choosing which partition
/// each event should be delivered to.
pub struct TopicProducer {
    inner: Arc<InnerTopicProducer>,
    #[cfg(feature = "smartengine")]
    sm_chain: Option<Arc<RwLock<fluvio_smartengine::SmartModuleChainInstance>>>,
    #[allow(unused)]
    metrics: Arc<ClientMetrics>,
}

struct InnerTopicProducer {
    config: Arc<TopicProducerConfig>,
    topic: String,
    spu_pool: Arc<SpuPool>,
    record_accumulator: RecordAccumulator,
    producer_pool: Arc<ProducerPool>,
}

impl InnerTopicProducer {
    /// Flush all the PartitionProducers and wait for them.
    async fn flush(&self) -> Result<()> {
        self.producer_pool.flush_all_batches().await?;
        Ok(())
    }

    async fn push_record(self: Arc<Self>, record: Record) -> Result<PushRecord> {
        let topics = self.spu_pool.metadata.topics();

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

        if let Some(error) = self.producer_pool.last_error(partition).await {
            return Err(error.into());
        }

        let push_record = self
            .record_accumulator
            .push_record(record, partition)
            .await?;

        Ok(push_record)
    }

    async fn clear_errors(&self) {
        self.producer_pool.clear_errors().await;
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

        impl TopicProducer {
            /// Adds a chain of SmartModules to this TopicProducer
            pub fn with_chain(mut self, chain_builder: SmartModuleChainBuilder) -> Result<Self> {
                let chain_instance = chain_builder.initialize(&SM_ENGINE).map_err(|e| FluvioError::Other(format!("SmartEngine - {e:?}")))?;
                self.sm_chain = Some(Arc::new(RwLock::new(chain_instance)));
                Ok(self)
            }

            /// Adds a SmartModule filter to this TopicProducer
            pub fn with_filter<T: Into<Vec<u8>>>(
                self,
                filter: T,
                params: BTreeMap<String, String>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder().params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, filter)))
            }

            /// Adds a SmartModule FilterMap to this TopicProducer
            pub fn with_filter_map<T: Into<Vec<u8>>>(
                self,
                map: T,
                params: BTreeMap<String, String>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder().params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, map)))
            }

            /// Adds a SmartModule map to this TopicProducer
            pub fn with_map<T: Into<Vec<u8>>>(
                self,
                map: T,
                params: BTreeMap<String, String>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder().params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, map)))
            }

            /// Adds a SmartModule array_map to this TopicProducer
            pub fn with_array_map<T: Into<Vec<u8>>>(
                self,
                map: T,
                params: BTreeMap<String, String>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder().params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, map)))
            }

            /// Adds a SmartModule aggregate to this TopicProducer
            pub fn with_aggregate<T: Into<Vec<u8>>>(
                self,
                map: T,
                params: BTreeMap<String, String>,
                accumulator: Vec<u8>,
            ) -> Result<Self> {
                let config = SmartModuleConfig::builder()
                    .initial_data(SmartModuleInitialData::Aggregate{accumulator})
                    .params(params.into()).build()?;
                self.with_chain(SmartModuleChainBuilder::from((config, map)))
            }

            /// Use generic smartmodule (the type is detected in smartengine)
            pub fn with_smartmodule<T: Into<Vec<u8>>>(
                self,
                smartmodule: T,
                params: BTreeMap<String, String>,
                context: SmartModuleContextData,
            ) -> Result<Self> {
                let mut config_builder = SmartModuleConfig::builder();
                config_builder.params(params.into());
                if let SmartModuleContextData::Aggregate{accumulator} = context {
                    config_builder.initial_data(SmartModuleInitialData::Aggregate{accumulator});
                };
                self.with_chain(SmartModuleChainBuilder::from((config_builder.build()?, smartmodule)))
            }

        }
    }
}

impl TopicProducer {
    pub(crate) async fn new(
        topic: String,
        spu_pool: Arc<SpuPool>,
        config: TopicProducerConfig,
        metrics: Arc<ClientMetrics>,
    ) -> Result<Self> {
        let config = Arc::new(config);
        let topics = spu_pool.metadata.topics();
        let topic_spec = topics
            .lookup_by_key(&topic)
            .await?
            .ok_or_else(|| FluvioError::TopicNotFound(topic.to_string()))?
            .spec;
        let partition_count = topic_spec.partitions();

        let compression =
            match topic_spec.get_compression_type() {
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
            CompressionAlgorithm::None => match config.compression {
                    Some(Compression::None) | None => Compression::None,
                    Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                        format!("Compression in the producer ({compression_config}) does not match with topic level compression (no compression)" )

                    )).into()),
                },
            };

        let record_accumulator = RecordAccumulator::new(
            config.batch_size,
            config.batch_queue_size,
            partition_count,
            compression,
        );
        let producer_pool = ProducerPool::shared(
            config.clone(),
            topic.clone(),
            spu_pool.clone(),
            record_accumulator.batches(),
            metrics.clone(),
        );

        Ok(Self {
            inner: Arc::new(InnerTopicProducer {
                config,
                topic,
                spu_pool,
                producer_pool,
                record_accumulator,
            }),
            #[cfg(feature = "smartengine")]
            sm_chain: Default::default(),
            metrics,
        })
    }

    /// Send all the queued records in the producer batches.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{TopicProducer, FluvioError};
    /// # async fn example(producer: &TopicProducer) -> anyhow::Result<()> {
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
    /// # use fluvio::{TopicProducer, FluvioError};
    /// # async fn example(producer: &TopicProducer) -> anyhow::Result<()> {
    /// producer.send("Key", "Value").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(
        skip(self, key, value),
        fields(topic = %self.inner.topic),
    )]
    pub async fn send<K, V>(&self, key: K, value: V) -> Result<ProduceOutput>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
    {
        let record_key = key.into();
        let record_value = value.into();
        let record = Record::from((record_key, record_value));

        cfg_if::cfg_if! {
            if #[cfg(feature = "smartengine")] {
                let mut entries = vec![record];

                use std::convert::TryFrom;
                use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

                let metrics = self.metrics.chain_metrics();

                if let Some(
                    smart_chain_ref
                ) = &self.sm_chain {
                    let mut sm_chain = smart_chain_ref.write().await;

                    let output = sm_chain.process(SmartModuleInput::try_from(entries)?,metrics).map_err(|e| FluvioError::Other(format!("SmartEngine - {e:?}")))?;
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
    pub async fn send_all<K, V, I>(&self, records: I) -> Result<Vec<ProduceOutput>>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
        I: IntoIterator<Item = (K, V)>,
    {
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

    #[cfg(feature = "stats")]
    /// Return a `ClientStatsDataFrame` to represent the current recorded client stats
    pub fn stats(&self) -> Option<ClientStatsDataFrame> {
        if self.inner.client_stats.stats_collect() != ClientStatsDataCollect::None {
            Some(self.inner.client_stats.get_dataframe())
        } else {
            None
        }
    }
}
