use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use tracing::{debug, instrument};
use async_channel::Sender;

mod dispatcher;
mod partitioning;

use fluvio_types::{SpuId, PartitionId};
use fluvio_types::event::StickyEvent;
use dataplane::ReplicaKey;
use dataplane::produce::DefaultProduceRequest;
use dataplane::produce::DefaultPartitionRequest;
use dataplane::produce::DefaultTopicRequest;
use dataplane::batch::{Batch, MemoryRecords};
use dataplane::record::Record;
pub use dataplane::record::{RecordKey, RecordData};

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::sync::StoreContext;
use crate::metadata::partition::PartitionSpec;
use crate::producer::partitioning::{Partitioner, PartitionerConfig, SiphashRoundRobinPartitioner};
pub(crate) use crate::producer::dispatcher::Dispatcher;

/// An interface for producing events to a particular topic
///
/// A `TopicProducer` allows you to send events to the specific
/// topic it was initialized for. Once you have a `TopicProducer`,
/// you can send events to the topic, choosing which partition
/// each event should be delivered to.
pub struct TopicProducer {
    topic: String,
    pool: Arc<SpuPool>,
    partitioner: Box<dyn Partitioner + Send + Sync>,
    inner: Arc<ProducerInner>,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct ProducerConfig {
    pub batch_duration: Duration,
    pub batch_size: usize,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_duration: Duration::from_millis(10),
            batch_size: 16_000,
        }
    }
}

/// A handle for `TopicProducer`s to communicate with the shared `ProducerDispatcher`.
///
/// When all `TopicProducer`s are dropped, the `ProducerInner` will drop and
/// notify the `ProducerDispatcher` to shutdown.
#[derive(Clone)]
pub(crate) struct ProducerInner {
    /// A channel for sending messages to the shared Producer Dispatcher.
    pub(crate) dispatcher: Sender<DispatcherMessage>,
    /// An event that can notify the Dispatcher to shutdown.
    pub(crate) dispatcher_shutdown: StickyEvent,
}

impl Drop for ProducerInner {
    fn drop(&mut self) {
        debug!("Notify dispatcher shutdown");
        self.dispatcher_shutdown.notify();
    }
}

impl TopicProducer {
    pub(crate) fn new(topic: String, pool: Arc<SpuPool>, config: ProducerConfig) -> Self {
        let partitioner = Box::new(SiphashRoundRobinPartitioner::new());

        let (sender, receiver) = async_channel::bounded(1);
        let dispatcher = Dispatcher::new(pool.clone(), receiver, config);
        let dispatcher_shutdown = dispatcher.start();
        let inner = Arc::new(ProducerInner {
            dispatcher: sender,
            dispatcher_shutdown,
        });

        Self {
            topic,
            pool,
            partitioner,
            inner,
        }
    }

    /// Sends a key/value record to this producer's Topic.
    ///
    /// The partition that the record will be sent to is derived from the Key.
    ///
    /// The `send` function does not wait for records to be committed to the SPU,
    /// it simply buffers them. In order to make sure that your records have all
    /// been actually sent, be sure to call [`TopicProducer::flush`] after sending
    /// all the records you have.
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
    /// ```
    #[instrument(
        skip(self, key, value),
        fields(topic = %self.topic),
    )]
    pub async fn send<K, V>(&self, key: K, value: V) -> Result<(), FluvioError>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
    {
        let key = key.into();
        let value = value.into();
        let record = Record::from((key, value));
        let pending_record = PendingRecord {
            partition: (),
            record,
            topic: self.topic.clone(),
        };
        let msg = DispatcherMessage::Record(pending_record);

        self.inner.dispatcher.send(msg).await.map_err(|_| {
            FluvioError::ProducerSend("failed to send record to dispatcher".to_string())
        })?;
        Ok(())
    }

    #[instrument(
        skip(self, records),
        fields(topic = %self.topic),
    )]
    pub async fn send_all<K, V, I>(&self, records: I) -> Result<(), FluvioError>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
        I: IntoIterator<Item = (K, V)>,
    {
        let topics = self.pool.metadata.topics();
        let topic_spec = topics
            .lookup_by_key(&self.topic)
            .await?
            .ok_or_else(|| FluvioError::TopicNotFound(self.topic.to_string()))?
            .spec;
        let partition_count = topic_spec.partitions();
        let partition_config = PartitionerConfig { partition_count };

        let entries = records
            .into_iter()
            .map::<(RecordKey, RecordData), _>(|(k, v)| (k.into(), v.into()))
            .map(Record::from);

        // Calculate the partition for each entry
        // Use a block scope to ensure we drop the partitioner lock
        let records_by_partition = {
            let mut iter = Vec::new();
            for record in entries {
                let key = record.key.as_ref().map(|k| k.as_ref());
                let value = record.value.as_ref();
                let partition = self.partitioner.partition(&partition_config, key, value);
                iter.push((partition, record));
            }
            iter
        };

        // Group all of the records by the partitions they belong to, then
        // group all of the partitions by the SpuId that leads that partition
        let partitions_by_spu = group_by_spu(
            &self.topic,
            self.pool.metadata.partitions(),
            records_by_partition,
        )
        .await?;

        // Create one request per SPU leader
        let requests = assemble_requests(&self.topic, partitions_by_spu);

        for (leader, request) in requests {
            let spu_client = self.pool.create_serial_socket_from_leader(leader).await?;
            spu_client.send_receive(request).await?;
        }

        Ok(())
    }

    /// Flushes any buffered records to the SPU, waiting until the whole buffer has been emptied.
    ///
    /// This should be used when you want to be sure that records have been sent
    /// before moving on to something else. If you are sending many records, you
    /// should _first_ [`send`] all the records, _then_ call flush.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::FluvioError;
    /// # async fn example() -> Result<(), FluvioError> {
    /// # let producer = fluvio::producer("topic").await?;
    /// for i in 0..100 {
    ///     producer.send(i.to_string(), format!("Hello, {}", i)).await?;
    /// }
    /// producer.flush().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn flush(&self) -> Result<(), FluvioError> {
        // Create a channel for the dispatcher to notify us when flushing is done
        let (tx, rx) = async_channel::bounded(1);
        self.inner
            .dispatcher
            .send(DispatcherMessage::Flush(tx))
            .await
            .map_err(|_| {
                FluvioError::ProducerFlush(
                    "failed to send flush command to producer dispatcher".to_string(),
                )
            })?;
        // Wait for flush to finish
        let _ = rx.recv().await;
        Ok(())
    }
}

async fn group_by_spu(
    topic: &str,
    partitions: &StoreContext<PartitionSpec>,
    records_by_partition: Vec<(PartitionId, Record)>,
) -> Result<HashMap<SpuId, HashMap<PartitionId, MemoryRecords>>, FluvioError> {
    let mut map: HashMap<SpuId, HashMap<i32, MemoryRecords>> = HashMap::new();
    for (partition, record) in records_by_partition {
        let replica_key = ReplicaKey::new(topic, partition);
        let partition_spec = partitions
            .lookup_by_key(&replica_key)
            .await?
            .ok_or_else(|| FluvioError::PartitionNotFound(topic.to_string(), partition))?
            .spec;
        let leader = partition_spec.leader;
        map.entry(leader)
            .or_insert_with(HashMap::new)
            .entry(partition)
            .or_insert_with(Vec::new)
            .push(record);
    }

    Ok(map)
}

fn assemble_requests(
    topic: &str,
    partitions_by_spu: HashMap<SpuId, HashMap<PartitionId, MemoryRecords>>,
) -> Vec<(SpuId, DefaultProduceRequest)> {
    let mut requests: Vec<(SpuId, DefaultProduceRequest)> =
        Vec::with_capacity(partitions_by_spu.len());

    for (leader, partitions) in partitions_by_spu {
        let mut request = DefaultProduceRequest::default();

        let mut topic_request = DefaultTopicRequest {
            name: topic.to_string(),
            ..Default::default()
        };

        for (partition, records) in partitions {
            let mut partition_request = DefaultPartitionRequest {
                partition_index: partition,
                ..Default::default()
            };
            partition_request.records.batches.push(Batch::from(records));
            topic_request.partitions.push(partition_request);
        }

        request.acks = 1;
        request.timeout_ms = 1500;
        request.topics.push(topic_request);
        requests.push((leader, request));
    }

    requests
}

pub enum DispatcherMessage {
    Record(PendingRecord<()>),
    Flush(Sender<std::convert::Infallible>),
}

/// Represents a Record sitting in the batch queue, waiting to be sent.
//
// Two type states: RecordPending<()> before we have calculated the partition, and
// RecordPending (i.e. RecordPending<PartitionId>) after we have calculated the partition.
// This way we avoid Option<PartitionId> and needing to unwrap.
pub struct PendingRecord<P = PartitionId> {
    partition: P,
    topic: String,
    record: Record,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::store::MetadataStoreObject;

    #[fluvio_future::test]
    async fn test_group_by_spu() {
        let partitions = StoreContext::new();
        let partition_specs: Vec<_> = vec![
            (ReplicaKey::new("TOPIC", 0), PartitionSpec::new(0, vec![])), // Partition 0
            (ReplicaKey::new("TOPIC", 1), PartitionSpec::new(0, vec![])), // Partition 1
            (ReplicaKey::new("TOPIC", 2), PartitionSpec::new(1, vec![])), // Partition 2
        ]
        .into_iter()
        .map(|(key, spec)| MetadataStoreObject::with_spec(key, spec))
        .collect();
        partitions.store().sync_all(partition_specs).await;
        let records_by_partition = vec![
            (0, Record::new("A")),
            (1, Record::new("B")),
            (2, Record::new("C")),
            (0, Record::new("D")),
            (1, Record::new("E")),
            (2, Record::new("F")),
        ];

        let grouped = group_by_spu("TOPIC", &partitions, records_by_partition)
            .await
            .unwrap();

        // SPU 0 should have partitions 0 and 1, but not 2
        let spu_0 = grouped.get(&0).unwrap();
        let partition_0 = spu_0.get(&0).unwrap();

        assert!(
            partition_0
                .iter()
                .any(|record| record.value.as_ref() == b"A"),
            "SPU 0/Partition 0 should contain record A",
        );
        assert!(
            partition_0
                .iter()
                .any(|record| record.value.as_ref() == b"D"),
            "SPU 0/Partition 0 should contain record D",
        );

        let partition_1 = spu_0.get(&1).unwrap();
        assert!(
            partition_1
                .iter()
                .any(|record| record.value.as_ref() == b"B"),
            "SPU 0/Partition 1 should contain record B",
        );
        assert!(
            partition_1
                .iter()
                .any(|record| record.value.as_ref() == b"E"),
            "SPU 0/Partition 1 should contain record E",
        );

        assert!(
            spu_0.get(&2).is_none(),
            "SPU 0 should not contain Partition 2",
        );

        let spu_1 = grouped.get(&1).unwrap();
        let partition_2 = spu_1.get(&2).unwrap();

        assert!(
            spu_1.get(&0).is_none(),
            "SPU 1 should not contain Partition 0",
        );

        assert!(
            spu_1.get(&1).is_none(),
            "SPU 1 should not contain Partition 1",
        );

        assert!(
            partition_2
                .iter()
                .any(|record| record.value.as_ref() == b"C"),
            "SPU 1/Partition 2 should contain record C",
        );

        assert!(
            partition_2
                .iter()
                .any(|record| record.value.as_ref() == b"F"),
            "SPU 1/Partition 2 should contain record F",
        );
    }

    #[test]
    fn test_assemble_requests() {
        let partitions_by_spu = {
            let mut pbs = HashMap::new();

            let spu_0 = {
                let mut s0_partitions = HashMap::new();
                let partition_0_records = vec![Record::new("A"), Record::new("B")];
                let partition_1_records = vec![Record::new("C"), Record::new("D")];
                s0_partitions.insert(0, partition_0_records);
                s0_partitions.insert(1, partition_1_records);
                s0_partitions
            };

            let spu_1 = {
                let mut s1_partitions = HashMap::new();
                let partition_2_records = vec![Record::new("E"), Record::new("F")];
                let partition_3_records = vec![Record::new("G"), Record::new("H")];
                s1_partitions.insert(2, partition_2_records);
                s1_partitions.insert(3, partition_3_records);
                s1_partitions
            };
            pbs.insert(0, spu_0);
            pbs.insert(1, spu_1);
            pbs
        };

        let requests = assemble_requests("TOPIC", partitions_by_spu);
        assert_eq!(requests.len(), 2);

        // SPU 0
        {
            let (spu0, request) = requests.iter().find(|(spu, _)| *spu == 0).unwrap();
            assert_eq!(*spu0, 0);
            assert_eq!(request.topics.len(), 1);
            let topic_request = request.topics.get(0).unwrap();
            assert_eq!(topic_request.name, "TOPIC");
            assert_eq!(topic_request.partitions.len(), 2);

            let partition_0_request = topic_request
                .partitions
                .iter()
                .find(|p| p.partition_index == 0)
                .unwrap();
            assert_eq!(partition_0_request.records.batches.len(), 1);
            let batch = partition_0_request.records.batches.get(0).unwrap();
            assert_eq!(batch.records().len(), 2);
            let record_0_0 = batch.records().get(0).unwrap();
            assert_eq!(record_0_0.value.as_ref(), b"A");
            let record_0_1 = batch.records().get(1).unwrap();
            assert_eq!(record_0_1.value.as_ref(), b"B");

            let partition_1_request = topic_request
                .partitions
                .iter()
                .find(|p| p.partition_index == 1)
                .unwrap();
            assert_eq!(partition_1_request.records.batches.len(), 1);
            let batch = partition_1_request.records.batches.get(0).unwrap();
            assert_eq!(batch.records().len(), 2);
            let record_1_0 = batch.records().get(0).unwrap();
            assert_eq!(record_1_0.value.as_ref(), b"C");
            let record_1_1 = batch.records().get(1).unwrap();
            assert_eq!(record_1_1.value.as_ref(), b"D");
        }

        // SPU 1
        {
            let (spu1, request) = requests.iter().find(|(spu, _)| *spu == 1).unwrap();
            assert_eq!(*spu1, 1);
            assert_eq!(request.topics.len(), 1);
            let topic_request = request.topics.get(0).unwrap();
            assert_eq!(topic_request.name, "TOPIC");
            assert_eq!(topic_request.partitions.len(), 2);

            let partition_0_request = topic_request
                .partitions
                .iter()
                .find(|p| p.partition_index == 2)
                .unwrap();
            assert_eq!(partition_0_request.records.batches.len(), 1);
            let batch = partition_0_request.records.batches.get(0).unwrap();
            assert_eq!(batch.records().len(), 2);
            let record_0_0 = batch.records().get(0).unwrap();
            assert_eq!(record_0_0.value.as_ref(), b"E");
            let record_0_1 = batch.records().get(1).unwrap();
            assert_eq!(record_0_1.value.as_ref(), b"F");

            let partition_1_request = topic_request
                .partitions
                .iter()
                .find(|p| p.partition_index == 3)
                .unwrap();
            assert_eq!(partition_1_request.records.batches.len(), 1);
            let batch = partition_1_request.records.batches.get(0).unwrap();
            assert_eq!(batch.records().len(), 2);
            let record_1_0 = batch.records().get(0).unwrap();
            assert_eq!(record_1_0.value.as_ref(), b"G");
            let record_1_1 = batch.records().get(1).unwrap();
            assert_eq!(record_1_1.value.as_ref(), b"H");
        }
    }

    #[test]
    fn test_construct_config() {
        let _config = ProducerConfig::default();
        let _config = ProducerConfig {
            batch_duration: std::time::Duration::from_millis(100),
            ..Default::default()
        };
    }
}
