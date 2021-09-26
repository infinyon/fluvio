use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument};
use async_channel::Sender;

mod dispatcher;
mod partitioning;

use fluvio_types::event::StickyEvent;
use dataplane::ReplicaKey;
use dataplane::record::Record;
pub use dataplane::record::{RecordKey, RecordData};

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::producer::partitioning::{Partitioner, PartitionerConfig, SiphashRoundRobinPartitioner};
pub(crate) use crate::producer::dispatcher::Dispatcher;

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

/// A handle for `TopicProducer`s to communicate with the shared `ProducerDispatcher`.
///
/// When all `TopicProducer`s are dropped, the `ProducerInner` will drop and
/// notify the `ProducerDispatcher` to shutdown.
#[derive(Clone)]
pub(crate) struct ProducerInner {
    /// A channel for sending messages to the shared Producer Dispatcher.
    pub(crate) dispatcher: Sender<ToDispatcher>,
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

        let topic_spec = self.pool.lookup_topic(&self.topic).await?.spec;
        let partition_count = topic_spec.partitions();
        let partition_config = PartitionerConfig { partition_count };

        let key_ref = record.key.as_ref().map(|it| it.as_ref());
        let partition =
            self.partitioner
                .partition(&partition_config, key_ref, record.value.as_ref());

        let replica_key = ReplicaKey::new(&self.topic, partition);
        let pending_record = PendingRecord {
            replica_key,
            record,
        };

        // Send this record to the dispatcher.
        let (to_producer, from_dispatcher) = async_channel::bounded(1);
        let msg = ToDispatcher::Record {
            record: pending_record,
            to_producer,
        };
        self.inner.dispatcher.send(msg).await.map_err(|_| {
            FluvioError::ProducerSend("failed to send record to dispatcher".to_string())
        })?;

        let dispatcher_result = from_dispatcher.recv().await;
        let retry_record = match dispatcher_result {
            // Err only occurs when the sender is dropped.
            // The dispatcher drops the sender when the record is SUCCESSFULLY added to the batch.
            Err(_) => return Ok(()),
            // If we get a message back, the dispatcher has REJECTED the record for some reason.
            // The most likely is that the buffer is full, and we should try again.
            Ok(record) => record,
        };

        let (to_producer, from_dispatcher) = async_channel::bounded(1);
        let msg = ToDispatcher::Record {
            record: retry_record,
            to_producer,
        };
        self.inner.dispatcher.send(msg).await.map_err(|_| {
            FluvioError::ProducerSend("failed to send record to dispatcher".to_string())
        })?;

        Ok(())
    }

    #[deprecated(since = "0.9.6", note = "use 'send' and 'flush' instead")]
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
    pub async fn flush(&self) -> Result<(), FluvioError> {
        // Create a channel for the dispatcher to notify us when flushing is done
        let (tx, rx) = async_channel::bounded(1);
        self.inner
            .dispatcher
            .send(ToDispatcher::Flush(tx))
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

pub enum ToDispatcher {
    Record {
        record: PendingRecord,
        to_producer: Sender<PendingRecord>,
    },
    Flush(Sender<std::convert::Infallible>),
}

/// Represents a Record sitting in the batch queue, waiting to be sent.
//
// Two type states: RecordPending<()> before we have calculated the partition, and
// RecordPending (i.e. RecordPending<PartitionId>) after we have calculated the partition.
// This way we avoid Option<PartitionId> and needing to unwrap.
pub struct PendingRecord {
    replica_key: ReplicaKey,
    record: Record,
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
