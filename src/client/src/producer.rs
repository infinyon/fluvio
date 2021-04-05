use std::io::Error as IoError;
use std::io::ErrorKind;
use std::sync::Arc;

use tracing::{debug, trace, instrument};
use dataplane::ReplicaKey;

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::sockets::SerialFrame;
use bytes::Bytes;

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
        self.send_all(Some((Some(key), value))).await?;
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
        let replica = ReplicaKey::new(&self.topic, 0);
        let spu_client = self.pool.create_serial_socket(&replica).await?;
        debug!(
            addr = spu_client.config().addr(),
            "Connected to replica leader:"
        );
        let records: Vec<_> = records
            .into_iter()
            .map(|(key, value): (Option<K>, V)| {
                let key = key.map(|k| Bytes::from(k.into()));
                let value = Bytes::from(value.into());
                (key, value)
            })
            .collect();
        send_records_raw(spu_client, &replica, records).await?;
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
    pub async fn send_record<B: AsRef<[u8]>>(
        &self,
        buffer: B,
        partition: i32,
    ) -> Result<(), FluvioError> {
        let record = buffer.as_ref();
        let replica = ReplicaKey::new(&self.topic, partition);
        debug!("sending records: {} bytes to: {}", record.len(), &replica);

        let spu_client = self.pool.create_serial_socket(&replica).await?;

        debug!("connect to replica leader at: {}", spu_client);

        let records = vec![(None, Bytes::from(Vec::from(buffer.as_ref())))];
        send_records_raw(spu_client, &replica, records).await?;
        Ok(())
    }
}

/// Sends record to a target server (Kf, SPU, or SC)
async fn send_records_raw<F: SerialFrame>(
    mut leader: F,
    replica: &ReplicaKey,
    records: Vec<(Option<Bytes>, Bytes)>,
) -> Result<(), FluvioError> {
    use dataplane::produce::DefaultProduceRequest;
    use dataplane::produce::DefaultPartitionRequest;
    use dataplane::produce::DefaultTopicRequest;
    use dataplane::batch::DefaultBatch;
    use dataplane::record::DefaultRecord;
    use dataplane::record::DefaultAsyncBuffer;

    // build produce log request message
    let mut request = DefaultProduceRequest::default();
    let mut topic_request = DefaultTopicRequest::default();
    let mut partition_request = DefaultPartitionRequest::default();

    debug!("Putting together batch with {} records", records.len());

    let records = records
        .into_iter()
        .map(|(key, value)| {
            let key = key.map(DefaultAsyncBuffer::new);
            let value = DefaultAsyncBuffer::new(value);
            DefaultRecord::from((key, value))
        })
        .collect();

    let batch = DefaultBatch::new(records);
    partition_request.partition_index = replica.partition;
    partition_request.records.batches.push(batch);
    topic_request.name = replica.topic.to_owned();
    topic_request.partitions.push(partition_request);

    request.acks = 1;
    request.timeout_ms = 1500;
    request.topics.push(topic_request);

    trace!("produce request: {:#?}", request);

    let response = leader.send_receive(request).await?;

    trace!("received response: {:?}", response);

    // process response
    match response.find_partition_response(&replica.topic, replica.partition) {
        Some(partition_response) => {
            if partition_response.error_code.is_error() {
                return Err(IoError::new(
                    ErrorKind::Other,
                    partition_response.error_code.to_sentence(),
                )
                .into());
            }
            Ok(())
        }
        None => Err(IoError::new(ErrorKind::Other, "unknown error").into()),
    }
}
