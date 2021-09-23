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

    #[test]
    fn test_construct_config() {
        let _config = ProducerConfig::default();
        let _config = ProducerConfig {
            batch_duration: std::time::Duration::from_millis(100),
            ..Default::default()
        };
    }
}
