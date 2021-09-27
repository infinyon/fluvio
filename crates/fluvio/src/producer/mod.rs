use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, instrument};
use async_channel::Sender;

mod error;
mod assoc;
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

const DEFAULT_BATCH_DURATION_MS: u64 = 10;
const DEFAULT_BATCH_SIZE_BYTES: usize = 16_000;

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
            batch_duration: Duration::from_millis(DEFAULT_BATCH_DURATION_MS),
            batch_size: DEFAULT_BATCH_SIZE_BYTES,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RecordUid(u64);

/// A monotonically increasing counter for assigning Uids to records
pub(crate) struct RecordUidGenerator {
    next_uid: std::sync::atomic::AtomicU64,
}

impl RecordUidGenerator {
    pub fn new() -> Self {
        Self {
            next_uid: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Claim the next UID in the monotonically increasing sequence
    pub fn claim(&self) -> RecordUid {
        let uid = self
            .next_uid
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    pub(crate) dispatcher_shutdown: StickyEvent,
    pub(crate) uid_generator: RecordUidGenerator,
    pub(crate) status_sender: tokio::sync::broadcast::Sender<BatchStatus>,
    pub(crate) status_receiver: Mutex<tokio::sync::broadcast::Receiver<BatchStatus>>,
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

        let (to_dispatcher, from_dispatcher) = async_channel::bounded(1);
        let (status_sender, status_receiver) = tokio::sync::broadcast::channel(100);
        let dispatcher =
            Dispatcher::new(pool.clone(), status_sender.clone(), from_dispatcher, config);
        let dispatcher_shutdown = dispatcher.start();
        let inner = Arc::new(ProducerInner {
            dispatcher: to_dispatcher,
            dispatcher_shutdown,
            uid_generator: RecordUidGenerator::new(),
            status_sender,
            status_receiver: Mutex::new(status_receiver),
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
            println!("SEND: CHECKING STATUSES");
            let mut receiver = self.inner.status_receiver.lock().expect("Mutex poisoned");
            while let Ok(status) = (*receiver).try_recv() {
                match status {
                    BatchStatus::Success(success) => {
                        println!(
                            "Batch succeeded at offset {} with records: {:#?}",
                            success.base_offset, success.batch
                        );
                    }
                    BatchStatus::Failure(failed) => {
                        println!(
                            "Batch failed with error {} with records: {:#?}",
                            failed.error_code.to_sentence(),
                            failed.batch
                        );
                        return Err(ProducerError::BatchFailed(failed).into());
                    }
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
        let (to_producer, from_dispatcher) = async_channel::bounded(1);
        let msg = DispatcherMsg::Record {
            record: pending_record,
            to_producer,
        };
        self.inner
            .dispatcher
            .send(msg)
            .await
            .map_err(ProducerError::from)?;

        let dispatcher_result = from_dispatcher.recv().await;
        match dispatcher_result {
            // In this case, an Err means that the Sender was dropped, which indicates success.
            // This makes sense if we think about this channel's primary use as being returning errors.
            Err(_) => {}
            Ok(ProducerMsg::RecordTooLarge(_)) => {
                return Err(ProducerError::RecordTooLarge.into());
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
        let (tx, rx) = async_channel::bounded(1);
        self.inner
            .dispatcher
            .send(DispatcherMsg::Flush(tx))
            .await
            .map_err(|_| ProducerError::Flush)?;
        // Wait for flush to finish
        let _ = rx.recv().await;
        Ok(())
    }
}

pub(crate) enum ProducerMsg {
    RecordTooLarge(AssociatedRecord),
}

pub(crate) enum DispatcherMsg {
    Record {
        record: AssociatedRecord,
        to_producer: Sender<ProducerMsg>,
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
