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

        // Split keys into a vec to feed to the partitioner
        let (keys, values): (Vec<Option<Vec<u8>>>, Vec<Vec<u8>>) = records
            .into_iter()
            .map(|(k, v)| (k.map(|it| it.into()), v.into()))
            .unzip();
        let key_slices: Vec<_> = keys.iter().map(|it| it.as_deref()).collect();

        let partitions = {
            // Ensure we drop partitioner (and its mutex guard) after we're done partitioning
            let mut partitioner = self.partitioner.lock().await;
            partitioner.update_config(partition_config);
            partitioner.partition(&key_slices)
        };

        // Zip up the partition assignments with their keys and values
        let iter: Vec<_> = partitions
            .into_iter()
            .zip(keys.into_iter().zip(values.into_iter()))
            .collect();

        // Convert key/values into records and sort them by partition
        let mut records_by_partition: HashMap<i32, Vec<_>> = HashMap::new();
        for (partition, (key, value)) in iter {
            let key = key.map(|it| DefaultAsyncBuffer::new(it));
            let record = DefaultAsyncBuffer::new(value);
            let record = DefaultRecord::from((key, record));

            records_by_partition
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(record);
        }

        // Create one request per partition
        // TODO collapse this into one request per leader (e.g. multiple partitions per leader)
        let mut requests = vec![];
        for (partition, records) in records_by_partition.into_iter() {
            let mut request = DefaultProduceRequest::default();
            let mut topic_request = DefaultTopicRequest::default();
            let mut partition_request = DefaultPartitionRequest::default();

            partition_request.partition_index = partition;
            partition_request
                .records
                .batches
                .push(DefaultBatch::new(records));
            topic_request.name = self.topic.clone();
            topic_request.partitions.push(partition_request);
            request.acks = 1;
            request.timeout_ms = 1500;
            request.topics.push(topic_request);

            requests.push((partition, request));
        }

        for (partition, request) in requests {
            let replica = ReplicaKey::new(&self.topic, partition);
            let mut spu_client = self.pool.create_serial_socket(&replica).await?;
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
    fn partition(&mut self, keys: &[Option<&[u8]>]) -> Vec<i32>;
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
    fn partition(&mut self, keys: &[Option<&[u8]>]) -> Vec<i32> {
        let mut partitions = Vec::with_capacity(keys.len());
        for maybe_key in keys {
            let partition = match maybe_key {
                Some(key) => partition_siphash(key, self.config.partition_count),
                None => {
                    let partition = self.index;
                    self.index = (self.index + 1) % self.config.partition_count;
                    partition
                }
            };
            partitions.push(partition);
        }
        partitions
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

        let key1_partition = partitioner.partition(&[None])[0];
        assert_eq!(key1_partition, 0);
        let key2_partition = partitioner.partition(&[None])[0];
        assert_eq!(key2_partition, 1);
        let key3_partition = partitioner.partition(&[None])[0];
        assert_eq!(key3_partition, 2);
        let key4_partition = partitioner.partition(&[None])[0];
        assert_eq!(key4_partition, 0);
        let key5_partition = partitioner.partition(&[None])[0];
        assert_eq!(key5_partition, 1);
        let key6_partition = partitioner.partition(&[None])[0];
        assert_eq!(key6_partition, 2);
    }

    /// Ensure that feeding keyless records in batches does not always start with the same partition
    #[test]
    fn test_round_robin_batch() {
        let config = PartitionerConfig { partition_count: 4 };
        let mut partitioner = SiphashRoundRobinPartitioner::new(config);

        // A batch of 5 records with no keys
        let batch: Vec<Option<&[u8]>> = (0..5).map(|_| None).collect();

        // The partitions of the first batch of five
        let ps1 = partitioner.partition(&batch);
        assert_eq!(ps1[0], 0);
        assert_eq!(ps1[1], 1);
        assert_eq!(ps1[2], 2);
        assert_eq!(ps1[3], 3);
        assert_eq!(ps1[4], 0);

        // The partitions of the second batch of five
        let ps2 = partitioner.partition(&batch);
        assert_eq!(ps2[0], 1); // resumes at 1
        assert_eq!(ps2[1], 2);
        assert_eq!(ps2[2], 3);
        assert_eq!(ps2[3], 0);
        assert_eq!(ps2[4], 1);
    }
}
