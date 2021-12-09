use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "smartengine")]
use async_lock::RwLock;
use async_lock::Mutex;

use dataplane::batch::Batch;
use dataplane::ReplicaKey;
use dataplane::record::Record;
use dataplane::produce::{DefaultProduceRequest, DefaultTopicRequest, DefaultPartitionRequest};

#[cfg(feature = "smartengine")]
use fluvio_smartengine::SmartModuleInstance;
use fluvio_types::PartitionId;
use futures_util::future::join_all;
use tracing::{instrument, error, trace};

mod accumulator;
mod config;
mod error;
mod output;
mod partitioning;
mod record;

pub use dataplane::record::{RecordKey, RecordData};

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::producer::accumulator::{RecordAccumulator, PushRecord, ProducerBatch};
use crate::producer::partitioning::{Partitioner, PartitionerConfig};

pub use self::config::{TopicProducerConfigBuilder, TopicProducerConfig};
pub use self::error::ProducerError;
pub use self::output::ProduceOutput;
pub use self::record::FutureRecordMetadata;

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

/// Struct that is responsible for sending produce requests to the SPU in a given partition.
struct PartitionProducer {
    replica: ReplicaKey,
    spu_pool: Arc<SpuPool>,
    batches_lock: Arc<Mutex<VecDeque<ProducerBatch>>>,
    linger: Duration,
}

impl PartitionProducer {
    fn new(
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches_lock: Arc<Mutex<VecDeque<ProducerBatch>>>,
        linger: Duration,
    ) -> Self {
        Self {
            replica,
            spu_pool,
            batches_lock,
            linger,
        }
    }

    fn shared(
        replica: ReplicaKey,
        spu_pool: Arc<SpuPool>,
        batches: Arc<Mutex<VecDeque<ProducerBatch>>>,
        linger: Duration,
    ) -> Arc<Self> {
        Arc::new(PartitionProducer::new(replica, spu_pool, batches, linger))
    }

    /// Flush all the batches that are full or have reached the linger time.
    /// If force is set to true, flush all batches regardless of linger time.
    async fn flush(&self, force: bool) -> Result<()> {
        let partition_spec = self
            .spu_pool
            .metadata
            .partitions()
            .lookup_by_key(&self.replica)
            .await?
            .ok_or_else(|| {
                FluvioError::PartitionNotFound(
                    self.replica.topic.to_string(),
                    self.replica.partition,
                )
            })?
            .spec;
        let leader = partition_spec.leader;

        let spu_socket = self
            .spu_pool
            .create_serial_socket_from_leader(leader)
            .await?;

        let mut batches_ready = vec![];
        let mut batches = self.batches_lock.lock().await;
        while !batches.is_empty() {
            let ready = force
                || batches.front().map_or(false, |batch| {
                    batch.is_full() || batch.create_time().elapsed() >= self.linger
                });
            if ready {
                if let Some(batch) = batches.pop_front() {
                    batches_ready.push(batch);
                }
            } else {
                break;
            }
        }

        // Send each batch and notify base offset
        for p_batch in batches_ready {
            let mut request = DefaultProduceRequest::default();

            let mut topic_request = DefaultTopicRequest {
                name: self.replica.topic.to_string(),
                ..Default::default()
            };
            let mut partition_request = DefaultPartitionRequest {
                partition_index: self.replica.partition,
                ..Default::default()
            };

            let batch_notifier = p_batch.notify;
            let batch = p_batch.records;

            let batch = Batch::from(batch);
            partition_request.records.batches.push(batch);

            topic_request.partitions.push(partition_request);
            request.acks = 1;
            request.timeout_ms = 1500;
            request.topics.push(topic_request);
            let response = spu_socket.send_receive(request).await?;

            let base_offset = response.responses[0].partitions[0].base_offset;

            if let Err(_e) = batch_notifier.send(base_offset).await {
                trace!(
                    "Failed to notify producer of successful produce because receiver was dropped"
                );
            }
        }

        Ok(())
    }
}

/// Pool of producers for a given topic. There is a producer per partition
struct ProducerPool {
    pool: Vec<Arc<PartitionProducer>>,
}

impl ProducerPool {
    fn new(
        topic: String,
        spu_pool: Arc<SpuPool>,
        batches: Arc<HashMap<PartitionId, Arc<Mutex<VecDeque<ProducerBatch>>>>>,
        linger: Duration,
    ) -> Self {
        let mut pool = vec![];
        for (&partition_id, batch_list) in batches.iter() {
            let replica = ReplicaKey::new(topic.clone(), partition_id);
            pool.push(PartitionProducer::shared(
                replica,
                spu_pool.clone(),
                batch_list.clone(),
                linger,
            ));
        }
        Self { pool }
    }

    fn shared(
        topic: String,
        spu_pool: Arc<SpuPool>,
        batches: Arc<HashMap<PartitionId, Arc<Mutex<VecDeque<ProducerBatch>>>>>,
        linger: Duration,
    ) -> Arc<Self> {
        Arc::new(ProducerPool::new(topic, spu_pool, batches, linger))
    }
}
struct InnerTopicProducer {
    topic: String,
    spu_pool: Arc<SpuPool>,
    partitioner: Box<dyn Partitioner + Send + Sync>,
    record_accumulator: RecordAccumulator,
    producer_pool: Arc<ProducerPool>,
    linger: Duration,
}

impl InnerTopicProducer {
    /// Flush all the PartitionProducers and wait for them.
    async fn flush(&self, force: bool) -> Result<()> {
        let mut futures = vec![];

        for producer in self.producer_pool.pool.iter() {
            let future = producer.flush(force);
            futures.push(future);
        }

        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

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

        let push_record = self
            .record_accumulator
            .push_record(record, partition)
            .await?;
        if push_record.is_full {
            self.flush(false).await?;
        }

        if push_record.new_batch {
            let linger = self.linger;
            fluvio_future::task::spawn(async move {
                fluvio_future::timer::sleep(linger).await;
                if let Err(e) = self.flush(false).await {
                    error!("Failed to flush producer: {:?}", e);
                }
            });
        }

        Ok(push_record)
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
        let record_accumulator = RecordAccumulator::new(config.batch_size, partition_count);
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
                linger: config.linger,
            }),
            #[cfg(feature = "smartengine")]
            smartmodule_instance: Default::default(),
        })
    }

    pub async fn flush(&self) -> Result<(), FluvioError> {
        self.inner.flush(true).await
    }

    /// Sends a key/value record to this producer's Topic.
    ///
    /// The partition that the record will be sent to is derived from the Key.
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
}
