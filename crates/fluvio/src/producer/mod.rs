use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument};

mod error;
mod assoc;
mod buffer;
mod dispatcher;
mod partitioning;

use fluvio_types::event::StickyEvent;
use dataplane::ReplicaKey;
use dataplane::record::Record;
pub use dataplane::record::{RecordKey, RecordData};

pub use error::ProducerError;
use crate::error::Result;
use crate::spu::SpuPool;
use crate::producer::partitioning::{Partitioner, PartitionerConfig, SiphashRoundRobinPartitioner};
pub(crate) use crate::producer::dispatcher::Dispatcher;
use crate::producer::assoc::{AssociatedRecord, BatchStatus};
use flume::Sender;
use crate::FluvioError;

const DEFAULT_BATCH_DURATION_MS: u64 = 20;
const DEFAULT_BATCH_SIZE_BYTES: usize = 16_000;

/// An interface for producing events to a particular topic
///
/// This will send records in batches behind the scenes in order to
/// increase throughput. In order to ensure that all records are sent,
/// you MUST call [`flush`] when you're done producing.
///
/// # Example
///
/// ```
/// use fluvio::{TopicProducer, FluvioError, RecordKey};
/// async fn example(producer: &TopicProducer) -> Result<(), FluvioError> {
///     producer.send(RecordKey::NULL, "Apple").await?;
///     producer.send(RecordKey::NULL, "Banana").await?;
///     producer.send(RecordKey::NULL, "Cranberry").await?;
///     producer.flush().await?; // IMPORTANT to ensure records are sent
///     Ok(())
/// }
/// ```
pub struct TopicProducer {
    topic: String,
    pool: Arc<SpuPool>,
    partitioner: Box<dyn Partitioner + Send + Sync>,
    inner: Arc<ProducerInner>,
}

/// Options used to adjust the behavior of the Producer.
///
/// # Example
///
/// Use this with `fluvio.topic_producer_with_config`.
///
/// ```
/// # use fluvio::{Fluvio, FluvioError, ProducerConfig};
/// # async fn example(fluvio: &Fluvio) -> Result<(), FluvioError> {
/// let config = ProducerConfig {
///     batch_duration: std::time::Duration::from_millis(10),
///     batch_size: 16_000,
///     // Add this to prevent breakage with future new fields
///     ..Default::default()
/// };
///
/// // Use config on this call:
/// fluvio.topic_producer_with_config("topic", config).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ProducerConfig {
    pub batch_duration: Duration,
    pub batch_size: usize,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_duration: Duration::from_millis(DEFAULT_BATCH_DURATION_MS),
            batch_size: DEFAULT_BATCH_SIZE_BYTES,
        }
    }
}

/// A unique identifier assigned to each Record sent to the Producer.
///
/// `RecordUid` is used to associate each input record with the eventual
/// success or failure status of the request the record belongs to.
/// Each call to `producer.send` returns a unique `RecordUid`, and each
/// request status includes the `RecordUid`s of all included records.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RecordUid(u64);

/// A monotonically increasing counter for assigning Uids to records
pub(crate) struct RecordUidGenerator(std::sync::atomic::AtomicU64);

impl RecordUidGenerator {
    pub fn new() -> Self {
        Self(std::sync::atomic::AtomicU64::new(0))
    }

    /// Claim the next UID in the monotonically increasing sequence
    pub fn claim(&self) -> RecordUid {
        let uid = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        RecordUid(uid)
    }
}

/// A handle for `TopicProducer`s to communicate with the shared `ProducerDispatcher`.
///
/// When all `TopicProducer`s are dropped, the `ProducerInner` will drop and
/// notify the `ProducerDispatcher` to shutdown.
pub(crate) struct ProducerInner {
    /// A channel for sending messages to the shared Producer Dispatcher.
    pub(crate) dispatcher: Sender<DispatcherMsg>,
    /// An event that can notify the Dispatcher to shutdown.
    pub(crate) dispatcher_shutdown: Arc<StickyEvent>,
    /// Generate monotonically-increasing `RecordUid`s.
    pub(crate) uid_generator: RecordUidGenerator,
    /// Used by the dispatcher to broadcast batch status notifications.
    // We hold this because we cannot clone a broadcast::Receiver,
    // we have to "subscribe" to a sender to get it
    pub(crate) _status_sender: tokio::sync::broadcast::Sender<BatchStatus>,
    /// Used by the producer to return batch statuses to the caller.
    pub(crate) status_receiver: async_lock::Mutex<tokio::sync::broadcast::Receiver<BatchStatus>>,
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

        let (to_dispatcher, from_dispatcher) = flume::bounded(0);
        let (status_sender, status_receiver) = tokio::sync::broadcast::channel(100);
        let dispatcher = Dispatcher::new(
            pool.clone(),
            status_sender.clone(),
            from_dispatcher,
            Arc::new(config),
        );
        let dispatcher_shutdown = dispatcher.start();
        let inner = Arc::new(ProducerInner {
            dispatcher: to_dispatcher,
            dispatcher_shutdown,
            uid_generator: RecordUidGenerator::new(),
            _status_sender: status_sender,
            status_receiver: async_lock::Mutex::new(status_receiver),
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
    pub async fn send<K, V>(&self, key: K, value: V) -> Result<RecordUid>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
    {
        let key = key.into();
        let value = value.into();
        let record = Record::from((key, value));

        {
            // Check if there have been any failures
            let mut receiver = self.inner.status_receiver.lock().await;
            while let Ok(status) = (*receiver).try_recv() {
                match status {
                    BatchStatus::Failure(failed) => {
                        return Err(ProducerError::BatchFailed(failed).into());
                    }
                    BatchStatus::InternalError(e) => {
                        return Err(FluvioError::Producer(ProducerError::Internal(e)));
                    }
                    // No action to take on success
                    _ => {}
                }
            }
        }

        let topic_spec = self.pool.lookup_topic(&self.topic).await?.spec;
        let partition_count = topic_spec.partitions();
        let partition_config = PartitionerConfig { partition_count };

        let key_ref = record.key.as_ref().map(|it| it.as_ref());
        let partition =
            self.partitioner
                .partition(&partition_config, key_ref, record.value.as_ref());

        let uid = self.inner.uid_generator.claim();
        let replica_key = ReplicaKey::new(&self.topic, partition);
        let pending_record = AssociatedRecord {
            uid: uid.clone(),
            replica_key,
            record,
        };

        // Send this record to the dispatcher.
        let (respond_to_producer, from_dispatcher) = flume::bounded(1);
        let msg = DispatcherMsg::Record {
            record: pending_record,
            respond_to_producer,
        };
        self.inner
            .dispatcher
            .send_async(msg)
            .await
            .map_err(ProducerError::from)?;

        let dispatcher_result = from_dispatcher.recv_async().await;
        match dispatcher_result {
            // In this case, an Err means that the Sender was dropped, which indicates success.
            // This makes sense if we think about this channel's primary use as being returning errors.
            Err(_) => {}
            Ok(ProducerMsg::RecordTooLarge(_, max_size)) => {
                return Err(ProducerError::RecordTooLarge(max_size).into());
            }
        }

        Ok(uid)
    }

    #[deprecated(since = "0.9.6", note = "use 'send' and 'flush' instead")]
    #[instrument(
        skip(self, records),
        fields(topic = %self.topic),
    )]
    pub async fn send_all<K, V, I>(&self, records: I) -> Result<()>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
        I: IntoIterator<Item = (K, V)>,
    {
        for (key, value) in records {
            self.send(key, value).await?;
        }
        self.flush().await?;

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
    pub async fn flush(&self) -> Result<()> {
        // Create a channel for the dispatcher to notify us when flushing is done
        let (tx, rx) = flume::bounded(1);
        self.inner
            .dispatcher
            .send_async(DispatcherMsg::Flush(tx))
            .await
            .map_err(|_| ProducerError::Flush)?;
        // Wait for flush to finish
        let _ = rx.recv_async().await;

        // If any batches come back with failed status, return Err
        {
            let mut receiver = self.inner.status_receiver.lock().await;
            while let Ok(status) = (*receiver).try_recv() {
                if let BatchStatus::Failure(failed) = status {
                    return Err(ProducerError::BatchFailed(failed).into());
                }
            }
        }
        Ok(())
    }
}

pub(crate) enum ProducerMsg {
    RecordTooLarge(AssociatedRecord, usize),
}

pub(crate) enum DispatcherMsg {
    Record {
        record: AssociatedRecord,
        respond_to_producer: Sender<ProducerMsg>,
    },
    Flush(Sender<std::convert::Infallible>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_config() {
        let _config = ProducerConfig::default();
        let _config = ProducerConfig {
            batch_duration: std::time::Duration::from_millis(100),
            ..Default::default()
        };
    }
}
