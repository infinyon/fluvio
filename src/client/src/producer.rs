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
use fluvio_types::SpuId;
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
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        self.send_all(Some((Some(key.into()), value))).await?;
        Ok(())
    }

    /// Sends a plain record with no key to this producer's Topic.
    ///
    /// The partition that the record will be sent to will be chosen in a round-robin fashion.
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
        skip(self, value),
        fields(topic = %self.topic),
    )]
    pub async fn send_keyless<V>(&self, value: V) -> Result<(), FluvioError>
    where
        V: Into<Vec<u8>>,
    {
        self.send_all(Some((None::<Vec<u8>>, value))).await?;
        Ok(())
    }

    #[instrument(
        skip(self, records),
        fields(topic = %self.topic),
    )]
    pub async fn send_all<K, V, I>(&self, records: I) -> Result<(), FluvioError>
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
        I: IntoIterator<Item = (Option<K>, V)>,
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
            .map::<(Option<Vec<u8>>, Vec<u8>), _>(|(k, v)| (k.map(|it| it.into()), v.into()))
            .map(|(key, value)| {
                let key = key.map(|it| DefaultAsyncBuffer::new(it));
                let value = DefaultAsyncBuffer::new(value);
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
                let partition = partitioner.partition(key);
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
        let mut requests: Vec<(SpuId, DefaultProduceRequest)> =
            Vec::with_capacity(partitions_by_spu.len());
        for (leader, partitions) in partitions_by_spu {
            let mut request = DefaultProduceRequest::default();

            let mut topic_request = DefaultTopicRequest::default();
            topic_request.name = self.topic.clone();

            for (partition, records) in partitions {
                let mut partition_request = DefaultPartitionRequest::default();
                partition_request.partition_index = partition;
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
    #[deprecated(since = "0.6.2", note = "Please use 'send_keyless' instead")]
    pub async fn send_record<B: AsRef<[u8]>>(
        &self,
        buffer: B,
        _partition: i32,
    ) -> Result<(), FluvioError> {
        let buffer: Vec<u8> = Vec::from(buffer.as_ref());
        self.send_keyless(buffer).await?;
        Ok(())
    }
}

async fn group_by_spu(
    topic: &str,
    partitions: &StoreContext<PartitionSpec>,
    records_by_partition: Vec<(i32, DefaultRecord)>,
) -> Result<HashMap<SpuId, HashMap<i32, Vec<DefaultRecord>>>, FluvioError> {
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
    fn partition(&mut self, key: Option<&[u8]>, value: &[u8]) -> i32;
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
    index: i32,
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

    /// Ensure that feeding keyless records one-at-a-time does not assign the same partition
    #[test]
    fn test_round_robin_individual() {
        let config = PartitionerConfig { partition_count: 3 };
        let mut partitioner = SiphashRoundRobinPartitioner::new(config);

        let key1_partition = partitioner.partition(None);
        assert_eq!(key1_partition, 0);
        let key2_partition = partitioner.partition(None);
        assert_eq!(key2_partition, 1);
        let key3_partition = partitioner.partition(None);
        assert_eq!(key3_partition, 2);
        let key4_partition = partitioner.partition(None);
        assert_eq!(key4_partition, 0);
        let key5_partition = partitioner.partition(None);
        assert_eq!(key5_partition, 1);
        let key6_partition = partitioner.partition(None);
        assert_eq!(key6_partition, 2);
    }
}
