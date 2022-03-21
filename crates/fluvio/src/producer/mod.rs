use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_lock::RwLock;

use dataplane::ReplicaKey;
use dataplane::record::Record;

use fluvio_compression::Compression;
use fluvio_sc_schema::topic::CompressionAlgorithm;
#[cfg(feature = "smartengine")]
use fluvio_smartengine::SmartModuleInstance;
use fluvio_types::PartitionId;
use fluvio_types::event::StickyEvent;
use tracing::instrument;

mod accumulator;
mod config;
mod error;
mod event;
mod output;
mod partitioning;
mod record;
mod partition_producer;

pub use dataplane::record::{RecordKey, RecordData};

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::producer::accumulator::{RecordAccumulator, PushRecord};
use crate::producer::partitioning::{Partitioner, PartitionerConfig};

use self::accumulator::{BatchHandler};
pub use self::config::{
    TopicProducerConfigBuilder, TopicProducerConfig, TopicProducerConfigBuilderError,
};
pub use self::error::ProducerError;
use self::event::EventHandler;
pub use self::output::ProduceOutput;
use self::partition_producer::PartitionProducer;
pub use self::record::{FutureRecordMetadata, RecordMetadata};

use crate::error::Result;

/// An interface for producing events to a particular topic
///
/// A `TopicProducer` allows you to send events to the specific
/// topic it was initialized for. Once you have a `TopicProducer`,
/// you can send events to the topic, choosing which partition
/// each event should be delivered to.
pub struct TopicProducer {
    inner: Arc<InnerTopicProducer>,
    #[cfg(feature = "smartengine")]
    pub(crate) smartmodule_instance: Option<Arc<RwLock<Box<dyn SmartModuleInstance>>>>,
}

/// Pool of producers for a given topic. There is a producer per partition
struct ProducerPool {
    flush_events: Vec<(Arc<EventHandler>, Arc<EventHandler>)>,
    end_events: Vec<Arc<StickyEvent>>,
    errors: Vec<Arc<RwLock<Option<ProducerError>>>>,
}

impl ProducerPool {
    fn new(
        topic: String,
        spu_pool: Arc<SpuPool>,
        batches: Arc<HashMap<PartitionId, BatchHandler>>,
        linger: Duration,
    ) -> Self {
        let mut end_events = vec![];
        let mut flush_events = vec![];
        let mut errors = vec![];
        for (&partition_id, (batch_events, batch_list)) in batches.iter() {
            let end_event = StickyEvent::shared();
            let flush_event = (EventHandler::shared(), EventHandler::shared());
            let replica = ReplicaKey::new(topic.clone(), partition_id);
            let error = Arc::new(RwLock::new(None));

            PartitionProducer::start(
                replica,
                spu_pool.clone(),
                batch_list.clone(),
                batch_events.clone(),
                linger,
                error.clone(),
                end_event.clone(),
                flush_event.clone(),
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
        topic: String,
        spu_pool: Arc<SpuPool>,
        batches: Arc<HashMap<PartitionId, BatchHandler>>,
        linger: Duration,
    ) -> Arc<Self> {
        Arc::new(ProducerPool::new(topic, spu_pool, batches, linger))
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
struct InnerTopicProducer {
    topic: String,
    spu_pool: Arc<SpuPool>,
    partitioner: Box<dyn Partitioner + Send + Sync>,
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
        let partition = self.partitioner.partition(&partition_config, key, value);

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
        use fluvio_spu_schema::server::stream_fetch::{SmartModuleWasmCompressed, SmartModuleKind, LegacySmartModulePayload};
        use std::collections::BTreeMap;
        use fluvio_smartengine::SmartEngine;

        impl TopicProducer {
            fn init_engine(&mut self, smart_payload: LegacySmartModulePayload) -> Result<(), FluvioError> {
                let engine = SmartEngine::default();
                let  smartmodule = engine.create_module_from_payload(
                    smart_payload,
                    None).map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;
                self.smartmodule_instance = Some(Arc::new(RwLock::new(smartmodule)));
                Ok(())
            }
            /// Adds a SmartModule filter to this TopicProducer
            pub fn with_filter<T: Into<Vec<u8>>>(
                mut self,
                filter: T,
                params: BTreeMap<String, String>,
            ) -> Result<Self, FluvioError> {
                let smart_payload = LegacySmartModulePayload {
                    wasm: SmartModuleWasmCompressed::Raw(filter.into()),
                    kind: SmartModuleKind::Filter,
                    params: params.into(),
                };
                self.init_engine(smart_payload)?;
                Ok(self)
            }

            /// Adds a SmartModule map to this TopicProducer
            pub fn with_map<T: Into<Vec<u8>>>(
                mut self,
                map: T,
                params: BTreeMap<String, String>,
            ) -> Result<Self, FluvioError> {
                let smart_payload = LegacySmartModulePayload {
                    wasm: SmartModuleWasmCompressed::Raw(map.into()),
                    kind: SmartModuleKind::Map,
                    params: params.into(),
                };
                self.init_engine(smart_payload)?;
                Ok(self)
            }

            /// Adds a SmartModule array_map to this TopicProducer
            pub fn with_array_map<T: Into<Vec<u8>>>(
                mut self,
                map: T,
                params: BTreeMap<String, String>,
            ) -> Result<Self, FluvioError> {
                let smart_payload = LegacySmartModulePayload {
                    wasm: SmartModuleWasmCompressed::Raw(map.into()),
                    kind: SmartModuleKind::ArrayMap,
                    params: params.into(),
                };

                self.init_engine(smart_payload)?;
                Ok(self)
            }

            /// Adds a SmartModule aggregate to this TopicProducer
            pub fn with_aggregate<T: Into<Vec<u8>>>(
                mut self,
                map: T,
                params: BTreeMap<String, String>,
                accumulator: Vec<u8>,
            ) -> Result<Self, FluvioError> {
                let smart_payload = LegacySmartModulePayload {
                    wasm: SmartModuleWasmCompressed::Raw(map.into()),
                    kind: SmartModuleKind::Aggregate { accumulator },
                    params: params.into(),
                };
                self.init_engine(smart_payload)?;
                Ok(self)
            }
        }
    }
}

impl TopicProducer {
    pub(crate) async fn new(
        topic: String,
        spu_pool: Arc<SpuPool>,
        config: TopicProducerConfig,
    ) -> Result<Self> {
        let partitioner = config.partitioner;
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
                        format!("Compression in the producer ({}) does not match with topic level compression (gzip)", compression_config),
                    ))),
                },
                CompressionAlgorithm::Snappy => match config.compression {
                    Some(Compression::Snappy) | None => Compression::Snappy,
                    Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                        format!("Compression in the producer ({}) does not match with topic level compression (snappy)", compression_config),
                    ))),
                },
                CompressionAlgorithm::Lz4 => match config.compression {
                    Some(Compression::Snappy) | None => Compression::Snappy,
                    Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                        format!("Compression in the producer ({}) does not match with topic level compression (lz4)", compression_config),
                    ))),
                },
            CompressionAlgorithm::None => match config.compression {
                    Some(Compression::Snappy) | None => Compression::Snappy,
                    Some(compression_config) => return Err(FluvioError::Producer(ProducerError::InvalidConfiguration(
                        format!("Compression in the producer ({}) does not match with topic level compression (no compression)", compression_config)

                    ))),
                },
            };

        let record_accumulator =
            RecordAccumulator::new(config.batch_size, partition_count, compression);
        let producer_pool = ProducerPool::shared(
            topic.clone(),
            spu_pool.clone(),
            record_accumulator.batches(),
            config.linger,
        );

        Ok(Self {
            inner: Arc::new(InnerTopicProducer {
                topic,
                spu_pool,
                partitioner,
                producer_pool,
                record_accumulator,
            }),
            #[cfg(feature = "smartengine")]
            smartmodule_instance: Default::default(),
        })
    }

    /// Send all the queued records in the producer batches.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{TopicProducer, FluvioError};
    /// # async fn example(producer: &TopicProducer) -> Result<(), FluvioError> {
    /// producer.send("Key", "Value").await?;
    /// producer.flush().await?;
    /// # Ok(())
    /// # }
    pub async fn flush(&self) -> Result<(), FluvioError> {
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
    /// # Example
    ///
    /// ```
    /// # use fluvio::{TopicProducer, FluvioError};
    /// # async fn example(producer: &TopicProducer) -> Result<(), FluvioError> {
    /// producer.send("Key", "Value").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(
        skip(self, key, value),
        fields(topic = %self.inner.topic),
    )]
    pub async fn send<K, V>(&self, key: K, value: V) -> Result<ProduceOutput, FluvioError>
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
                use dataplane::smartmodule::SmartModuleInput;
                use std::convert::TryFrom;

                if let Some(
                    smartmodule_instance_ref
                ) = &self.smartmodule_instance {
                    let mut smartengine = smartmodule_instance_ref.write().await;
                    let output = smartengine.process(SmartModuleInput::try_from(entries)?).map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;
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
    pub async fn send_all<K, V, I>(&self, records: I) -> Result<Vec<ProduceOutput>, FluvioError>
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
}
