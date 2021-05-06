use std::sync::Arc;
use std::collections::HashMap;
use tracing::instrument;
use siphasher::sip::SipHasher;
use async_mutex::Mutex;

use dataplane::ReplicaKey;
use dataplane::produce::DefaultProduceRequest;
use dataplane::produce::DefaultPartitionRequest;
use dataplane::produce::DefaultTopicRequest;
use dataplane::batch::DefaultBatch;
use dataplane::record::DefaultRecord;
use dataplane::record::DefaultAsyncBuffer;

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::sockets::SerialFrame;
use fluvio_types::{SpuId, PartitionId};
use crate::sync::StoreContext;
use crate::metadata::partition::PartitionSpec;

/// An interface for producing events to a particular topic
///
/// A `TopicProducer` allows you to send events to the specific
/// topic it was initialized for. Once you have a `TopicProducer`,
/// you can send events to the topic, choosing which partition
/// each event should be delivered to.
pub struct TopicProducer {
    topic: String,
    pool: Arc<SpuPool>,
    partitioner: Arc<Mutex<dyn Partitioner + Send + Sync>>,
}

impl TopicProducer {
    pub(crate) fn new(topic: String, pool: Arc<SpuPool>) -> Self {
        let config = PartitionerConfig { partition_count: 1 };
        let partitioner = Arc::new(Mutex::new(SiphashRoundRobinPartitioner::new(config)));
        Self {
            topic,
            pool,
            partitioner,
        }
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
        fields(topic = %self.topic),
    )]
    pub async fn send<K, V>(&self, key: K, value: V) -> Result<(), FluvioError>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
    {
        let record_key = key.into();
        let record_value = value.into();
        self.send_all(Some((record_key, record_value))).await?;
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
            .map(|(key, value)| {
                let key = match key.0 {
                    RecordKeyInner::Null => None,
                    RecordKeyInner::Key(key) => Some(DefaultAsyncBuffer::new(key.0)),
                };
                let value = DefaultAsyncBuffer::new(value.0);
                DefaultRecord::from((key, value))
            });

        // Calculate the partition for each entry
        // Use a block scope to ensure we drop the partitioner lock
        let records_by_partition = {
            let mut iter = Vec::new();
            let mut partitioner = self.partitioner.lock().await;
            partitioner.update_config(partition_config);

            for record in entries {
                let key = record.key.as_ref().map(|k| k.as_ref());
                let value = record.value.as_ref();
                let partition = partitioner.partition(key, value);
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
            let mut spu_client = self.pool.create_serial_socket_from_leader(leader).await?;
            spu_client.send_receive(request).await?;
        }

        Ok(())
    }

    /// Sends an event to a specific partition within this producer's topic
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{TopicProducer, FluvioError};
    /// # async fn do_send_record(producer: &TopicProducer) -> Result<(), FluvioError> {
    /// let partition = 0;
    /// producer.send_record("Hello, Fluvio!", partition).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(
        skip(self, buffer),
        fields(topic = %self.topic),
    )]
    #[deprecated(since = "0.6.2", note = "Use 'send' instead")]
    pub async fn send_record<B: AsRef<[u8]>>(
        &self,
        buffer: B,
        _partition: i32,
    ) -> Result<(), FluvioError> {
        let buffer: Vec<u8> = Vec::from(buffer.as_ref());
        self.send_all(Some((RecordKey::NULL, buffer))).await?;
        Ok(())
    }
}

async fn group_by_spu(
    topic: &str,
    partitions: &StoreContext<PartitionSpec>,
    records_by_partition: Vec<(PartitionId, DefaultRecord)>,
) -> Result<HashMap<SpuId, HashMap<PartitionId, Vec<DefaultRecord>>>, FluvioError> {
    let mut map: HashMap<SpuId, HashMap<i32, Vec<DefaultRecord>>> = HashMap::new();
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
    partitions_by_spu: HashMap<SpuId, HashMap<PartitionId, Vec<DefaultRecord>>>,
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
            partition_request
                .records
                .batches
                .push(DefaultBatch::new(records));
            topic_request.partitions.push(partition_request);
        }

        request.acks = 1;
        request.timeout_ms = 1500;
        request.topics.push(topic_request);
        requests.push((leader, request));
    }

    requests
}

/// A key for determining which partition a record should be sent to.
///
/// This type is used to support conversions from any other type that
/// may be converted to a `Vec<u8>`, while still allowing the ability
/// to explicitly state that a record may have no key (`RecordKey::NULL`).
///
/// # Examples
///
/// ```
/// # use fluvio::{TopicProducer, FluvioError, RecordKey};
/// # async fn example(producer: &TopicProducer) -> Result<(), FluvioError> {
/// producer.send("Hello", String::from("World!")).await?;
/// producer.send(RecordKey::NULL, "World!").await?;
/// # Ok(())
/// # }
/// ```
pub struct RecordKey(RecordKeyInner);

impl RecordKey {
    pub const NULL: Self = Self(RecordKeyInner::Null);
}

enum RecordKeyInner {
    Null,
    Key(RecordData),
}

impl<K: Into<Vec<u8>>> From<K> for RecordKey {
    fn from(k: K) -> Self {
        Self(RecordKeyInner::Key(RecordData::from(k)))
    }
}

/// A type to hold the contents of a record's value.
pub struct RecordData(bytes::Bytes);

impl<V: Into<Vec<u8>>> From<V> for RecordData {
    fn from(v: V) -> Self {
        let value: Vec<u8> = v.into();
        Self(value.into())
    }
}

/// A trait for defining a partitioning strategy for key/value records.
///
/// A Partitioner is given a slice of potential keys, and the number of
/// partitions in the current Topic. It must map each key from the input
/// slice into a partition stored at the same index in the output Vec.
///
/// It is up to the implementor to decide how the keys get mapped to
/// partitions. This includes deciding what partition to assign to records
/// with no keys (represented by `None` values in the keys slice).
///
/// See [`SiphashRoundRobinPartitioner`] for a reference implementation.
trait Partitioner {
    fn partition(&mut self, key: Option<&[u8]>, value: &[u8]) -> PartitionId;
    fn update_config(&mut self, config: PartitionerConfig);
}

struct PartitionerConfig {
    partition_count: i32,
}

/// A [`Partitioner`] which combines hashing and round-robin partition assignment
///
/// - Records with keys get their keys hashed with siphash
/// - Records without keys get assigned to partitions using round-robin
struct SiphashRoundRobinPartitioner {
    index: PartitionId,
    config: PartitionerConfig,
}

impl SiphashRoundRobinPartitioner {
    pub fn new(config: PartitionerConfig) -> Self {
        Self { index: 0, config }
    }
}

impl Partitioner for SiphashRoundRobinPartitioner {
    fn partition(&mut self, maybe_key: Option<&[u8]>, _value: &[u8]) -> i32 {
        match maybe_key {
            Some(key) => partition_siphash(key, self.config.partition_count),
            None => {
                let partition = self.index;
                self.index = (self.index + 1) % self.config.partition_count;
                partition
            }
        }
    }

    fn update_config(&mut self, config: PartitionerConfig) {
        self.config = config;
    }
}

fn partition_siphash(key: &[u8], partition_count: i32) -> i32 {
    use std::hash::{Hash, Hasher};
    use std::convert::TryFrom;

    assert!(partition_count >= 0, "Partition must not be less than zero");
    let mut hasher = SipHasher::new();
    key.hash(&mut hasher);
    let hashed = hasher.finish();

    i32::try_from(hashed % partition_count as u64).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::store::MetadataStoreObject;

    /// Ensure that feeding keyless records one-at-a-time does not assign the same partition
    #[test]
    fn test_round_robin_individual() {
        let config = PartitionerConfig { partition_count: 3 };
        let mut partitioner = SiphashRoundRobinPartitioner::new(config);

        let key1_partition = partitioner.partition(None, &[]);
        assert_eq!(key1_partition, 0);
        let key2_partition = partitioner.partition(None, &[]);
        assert_eq!(key2_partition, 1);
        let key3_partition = partitioner.partition(None, &[]);
        assert_eq!(key3_partition, 2);
        let key4_partition = partitioner.partition(None, &[]);
        assert_eq!(key4_partition, 0);
        let key5_partition = partitioner.partition(None, &[]);
        assert_eq!(key5_partition, 1);
        let key6_partition = partitioner.partition(None, &[]);
        assert_eq!(key6_partition, 2);
    }

    #[fluvio_future::test_async]
    async fn test_group_by_spu() -> Result<(), ()> {
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
            (0, DefaultRecord::new("A")),
            (1, DefaultRecord::new("B")),
            (2, DefaultRecord::new("C")),
            (0, DefaultRecord::new("D")),
            (1, DefaultRecord::new("E")),
            (2, DefaultRecord::new("F")),
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

        Ok(())
    }

    #[test]
    fn test_assemble_requests() {
        let partitions_by_spu = {
            let mut pbs = HashMap::new();

            let spu_0 = {
                let mut s0_partitions = HashMap::new();
                let partition_0_records = vec![DefaultRecord::new("A"), DefaultRecord::new("B")];
                let partition_1_records = vec![DefaultRecord::new("C"), DefaultRecord::new("D")];
                s0_partitions.insert(0, partition_0_records);
                s0_partitions.insert(1, partition_1_records);
                s0_partitions
            };

            let spu_1 = {
                let mut s1_partitions = HashMap::new();
                let partition_2_records = vec![DefaultRecord::new("E"), DefaultRecord::new("F")];
                let partition_3_records = vec![DefaultRecord::new("G"), DefaultRecord::new("H")];
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
}
