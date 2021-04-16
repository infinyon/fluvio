use std::sync::Arc;
use std::collections::HashMap;
use tracing::instrument;
use siphasher::sip::SipHasher;

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
}

impl TopicProducer {
    pub(crate) fn new(topic: String, pool: Arc<SpuPool>) -> Self {
        Self { topic, pool }
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
        let topic_spec = topics.lookup_by_key(&self.topic).await?.unwrap().spec; // TODO fix unwrap and stuff
        let partition_count = topic_spec.partitions();

        let records: Vec<(Option<Vec<u8>>, Vec<u8>)> = records
            .into_iter()
            .map(|(k, v)| (k.map(|it| it.into()), v.into()))
            .collect();

        let mut round_robin = 0i32;
        let mut records_by_partition: HashMap<i32, Vec<_>> = HashMap::new();
        for (key, value) in records.into_iter() {
            let partition = match &key {
                Some(key) => partition_siphash(key, partition_count),
                None => {
                    let partition = round_robin;
                    round_robin = (round_robin + 1) % partition_count;
                    partition
                }
            };
            println!("Assigned partition: {}", partition);

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

fn partition_siphash(key: &[u8], partition_count: i32) -> i32 {
    use std::hash::{Hash, Hasher};
    use std::convert::TryFrom;

    assert!(partition_count >= 0, "Partition must not be less than zero");
    let mut hasher = SipHasher::new();
    key.hash(&mut hasher);
    let hashed = hasher.finish();

    // TODO sanity check that this makes sense
    i32::try_from(hashed % partition_count as u64).unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_u64_mod_i32() {
        assert_eq!(i32::MAX as u64, 2147483647_u64);
        let some_u64 = 2u64.pow(63);
        let some_u64_mod_i32 = some_u64 % (i32::MAX as u64);
        assert!(some_u64_mod_i32 < i32::MAX as u64);
        let some_i32 = some_u64_mod_i32 as i32;
        println!(
            "u64: {}, u64_mod: {}, i32: {}",
            some_u64, some_u64_mod_i32, some_i32
        );
    }
}
