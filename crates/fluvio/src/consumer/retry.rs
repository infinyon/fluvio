use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;
use adaptive_backoff::prelude::{
    Backoff, BackoffBuilder, ExponentialBackoff, ExponentialBackoffBuilder,
};
use fluvio_socket::ClientConfig;
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use fluvio_future::timer::sleep;
use fluvio_protocol::record::ConsumerRecord;
use fluvio_sc_schema::errors::ErrorCode;

use crate::consumer::RetryMode;
use crate::{Fluvio, FluvioClusterConfig, Offset};
use super::{ConsumerConfigExt, ConsumerStream, ConsumerBoxFuture};

pub const SPAN_RETRY: &str = "fluvio::retry";
pub const BACKOFF_MIN_DURATION: Duration = Duration::from_secs(1);
pub const BACKOFF_MAX_DURATION: Duration = Duration::from_secs(30);
pub const BACKOFF_FACTOR: f64 = 1.1;

/// Type alias for the consumer record stream.
#[cfg(target_arch = "wasm32")]
type BoxConsumerStream =
    Pin<Box<dyn ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + 'static>>;
#[cfg(not(target_arch = "wasm32"))]
type BoxConsumerStream =
    Pin<Box<dyn ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + Send + 'static>>;

/// Type alias for the future returned by our retry logic.
#[cfg(target_arch = "wasm32")]
type BoxConsumerFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    BoxConsumerStream,
                    Option<Result<(ConsumerRecord, Option<i64>), ErrorCode>>,
                ),
            > + 'static,
    >,
>;
#[cfg(not(target_arch = "wasm32"))]
type BoxConsumerFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    BoxConsumerStream,
                    Option<Result<(ConsumerRecord, Option<i64>), ErrorCode>>,
                ),
            > + Send
            + 'static,
    >,
>;

#[derive(Clone)]
pub struct ConsumerRetryInner {
    cluster_config: FluvioClusterConfig,
    next_offset_to_read: Option<i64>,
    consumer_config: ConsumerConfigExt,
    client_config: Arc<ClientConfig>,
}

/// The internal state of our consumer.
enum ConsumerRetryState {
    /// The stream is idle and ready to consume.
    Idle,
    /// The stream is currently processing a task.
    Task(BoxConsumerFuture),
    /// The stream has terminated.
    Terminated,
}

/// A consumer stream that automatically retries on failure.
///
/// In this refactored version we remove the mutex by taking ownership of
/// the consumer stream whenever we start a new retry task. When the task finishes,
/// the stream is returned.
pub struct ConsumerRetryStream {
    inner: ConsumerRetryInner,
    state: ConsumerRetryState,
    /// The consumer stream is stored directly (inside an Option for ownership transfer).
    stream: Option<BoxConsumerStream>,
    notify: Arc<Notify>,
}

impl ConsumerRetryStream {
    fn change_state(&mut self, new_state: ConsumerRetryState) {
        self.state = new_state;
        self.notify.notify_one();
    }

    fn set_idle(&mut self) {
        self.change_state(ConsumerRetryState::Idle);
        self.notify.notify_one();
    }

    fn set_terminated(&mut self) {
        self.change_state(ConsumerRetryState::Terminated);
        self.notify.notify_one();
    }

    fn set_task(&mut self, task: BoxConsumerFuture) {
        self.change_state(ConsumerRetryState::Task(task));
    }
}

impl Stream for ConsumerRetryStream {
    type Item = Result<ConsumerRecord, ErrorCode>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                ConsumerRetryState::Terminated => return Poll::Ready(None),
                ConsumerRetryState::Idle => {
                    // Take ownership of the stream and start a new retry task.
                    if let Some(stream) = self.stream.take() {
                        let future = Self::consumer_with_retry(self.inner.clone(), stream);
                        self.set_task(Box::pin(future));
                    } else {
                        // If the stream is missing, treat as terminated.
                        self.set_terminated();
                        return Poll::Ready(None);
                    }
                }
                ConsumerRetryState::Task(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready((new_stream, opt_result)) => {
                        self.stream = Some(new_stream);
                        self.set_idle();
                        match opt_result {
                            Some(Ok((record, new_offset))) => {
                                self.inner.next_offset_to_read = new_offset;
                                return Poll::Ready(Some(Ok(record)));
                            }
                            Some(Err(e)) => {
                                self.notify.notify_one();
                                return Poll::Ready(Some(Err(e)));
                            }
                            None => {
                                self.set_terminated();
                                return Poll::Ready(None);
                            }
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

impl ConsumerStream for ConsumerRetryStream {
    fn offset_commit(&mut self) -> ConsumerBoxFuture {
        let notify = self.notify.clone();
        Box::pin(async move {
            loop {
                match self.state {
                    ConsumerRetryState::Idle | ConsumerRetryState::Terminated => {
                        if let Some(ref mut stream) = self.stream {
                            return stream.offset_commit().await;
                        } else {
                            return Err(ErrorCode::Other("Stream not available".to_string()));
                        }
                    }
                    _ => {
                        notify.notified().await;
                    }
                }
            }
        })
    }

    fn offset_flush(&mut self) -> ConsumerBoxFuture {
        let notify = self.notify.clone();
        Box::pin(async move {
            loop {
                match self.state {
                    ConsumerRetryState::Idle | ConsumerRetryState::Terminated => {
                        if let Some(ref mut stream) = self.stream {
                            return stream.offset_flush().await;
                        } else {
                            return Err(ErrorCode::Other("Stream not available".to_string()));
                        }
                    }
                    _ => {
                        notify.notified().await;
                    }
                }
            }
        })
    }
}

impl ConsumerRetryStream {
    /// Creates a new `ConsumerRetryStream` with the given configuration.
    pub async fn new(
        fluvio: &Fluvio,
        cluster_config: FluvioClusterConfig,
        config: ConsumerConfigExt,
    ) -> Result<Self> {
        let client_config = fluvio.client_config();
        let stream = fluvio.consumer_with_config_inner(config.clone()).await?;
        let boxed_stream: BoxConsumerStream = Box::pin(stream);

        Ok(Self {
            inner: ConsumerRetryInner {
                client_config,
                cluster_config,
                next_offset_to_read: None,
                consumer_config: config,
            },
            state: ConsumerRetryState::Idle,
            stream: Some(boxed_stream),
            notify: Arc::new(Notify::new()),
        })
    }

    /// Consumes records from the stream with retry logic.
    ///
    /// On error or end-of-stream, it waits (using exponential backoff) and then
    /// reconnects. When a record is successfully produced, the new stream and
    /// updated offset are returned.
    async fn consumer_with_retry(
        inner: ConsumerRetryInner,
        mut stream: BoxConsumerStream,
    ) -> (
        BoxConsumerStream,
        Option<Result<(ConsumerRecord, Option<i64>), ErrorCode>>,
    ) {
        let mut attempts: u32 = 0;
        let mut backoff = match create_backoff() {
            Ok(b) => b,
            Err(_) => {
                return (
                    stream,
                    Some(Err(ErrorCode::Other("Error creating backoff".to_string()))),
                );
            }
        };

        loop {
            // Try to retrieve the next record.
            if let Some(record_result) = stream.as_mut().next().await {
                match record_result {
                    Ok(record) => {
                        let new_offset = Some(record.offset + 1);
                        if attempts > 0 {
                            debug!(target: SPAN_RETRY, "Record produced successfully after reconnect");
                        }
                        return (stream, Some(Ok((record, new_offset))));
                    }
                    Err(e) => {
                        warn!(target: SPAN_RETRY, "Error consuming record: {}", e);
                        if let RetryMode::Disabled = inner.consumer_config.retry_mode {
                            return (stream, Some(Err(e)));
                        }
                    }
                }
            }

            // If continuous consumption is disabled, end the stream.
            if inner.consumer_config.disable_continuous {
                return (stream, None);
            }

            // Wait before retrying.
            backoff_and_wait(&mut backoff).await;
            attempts += 1;

            // Determine the offset for reconnection.
            let offset = if let Some(next) = inner.next_offset_to_read {
                match Offset::absolute(next) {
                    Ok(off) => off,
                    Err(e) => {
                        warn!(target: SPAN_RETRY, "Error creating offset: {}", e);
                        return (stream, Some(Err(ErrorCode::OffsetOutOfRange)));
                    }
                }
            } else {
                inner.consumer_config.offset_start.clone()
            };

            // Update the consumer configuration with the new offset.
            let mut new_config = inner.consumer_config.clone();
            new_config.offset_start = offset;

            // Reconnect loop: keep trying until a new stream is created.
            loop {
                match Self::reconnect_stream(&inner, new_config.clone(), backoff.clone()).await {
                    Ok(new_stream) => {
                        info!(target: SPAN_RETRY, "Created new consumer stream with offset: {:?}", new_config.offset_start);
                        stream = new_stream;
                        break;
                    }
                    Err(e) => {
                        backoff_and_wait(&mut backoff).await;
                        attempts += 1;
                        warn!(target: SPAN_RETRY, "Could not connect to stream on {}: {}", inner.consumer_config.topic, e);

                        match inner.consumer_config.retry_mode {
                            RetryMode::TryUntil(max) if attempts >= max => {
                                return (stream, Some(Err(ErrorCode::MaxRetryReached)));
                            }
                            RetryMode::Disabled => {
                                return (stream, Some(Err(ErrorCode::Other(format!("{}", e)))));
                            }
                            _ => {} // Continue retrying.
                        }
                    }
                }
            }
        }
    }

    async fn reconnect_stream(
        inner: &ConsumerRetryInner,
        new_config: ConsumerConfigExt,
        mut backoff: ExponentialBackoff,
    ) -> Result<BoxConsumerStream> {
        info!(target: SPAN_RETRY, "Reconnecting to stream consumer");
        let fluvio_client = Fluvio::connect_with_connector(
            inner.client_config.connector().clone(),
            &inner.cluster_config,
        )
        .await?;

        let new_stream = fluvio_client
            .consumer_with_config_inner(new_config.clone())
            .await?;

        backoff.reset();
        Ok(Box::pin(new_stream))
    }
}

/// Creates an exponential backoff configuration.
fn create_backoff() -> Result<ExponentialBackoff> {
    ExponentialBackoffBuilder::default()
        .factor(BACKOFF_FACTOR)
        .min(BACKOFF_MIN_DURATION)
        .max(BACKOFF_MAX_DURATION)
        .build()
}

/// Waits for the duration determined by the exponential backoff.
async fn backoff_and_wait(backoff: &mut ExponentialBackoff) {
    let wait_duration = backoff.wait();
    info!(target: SPAN_RETRY, seconds = wait_duration.as_secs(), "Starting backoff: sleeping for duration");
    let _ = sleep(wait_duration).await;
    debug!(target: SPAN_RETRY, "Resuming after backoff");
}

#[cfg(test)]
mod tests {
    use std::vec::IntoIter;

    use fluvio_protocol::record::Batch;
    use fluvio_smartmodule::RecordData;
    use fluvio_types::PartitionId;
    use futures_util::{stream::Iter, StreamExt};

    use crate::consumer::{
        MultiplePartitionConsumerStream, OffsetManagementStrategy, SinglePartitionConsumerStream,
        StreamToServer,
    };

    use super::*;

    #[fluvio_future::test]
    async fn test_retry_stream() {
        //given
        let (tx1, rx1) = async_channel::unbounded();
        let partition_stream1 = SinglePartitionConsumerStream::new(
            records_stream(0, ["1", "3", "5"]),
            OffsetManagementStrategy::Manual,
            Default::default(),
            tx1,
        );
        let (tx2, rx2) = async_channel::unbounded();
        let partition_stream2 = SinglePartitionConsumerStream::new(
            records_stream(1, ["2", "4", "6"]),
            OffsetManagementStrategy::Manual,
            Default::default(),
            tx2,
        );
        let multi_stream =
            MultiplePartitionConsumerStream::new([partition_stream1, partition_stream2]);

        let mut retry_stream = ConsumerRetryStream {
            inner: ConsumerRetryInner {
                client_config: Arc::new(ClientConfig::with_addr("localhost:9010".to_string())),
                cluster_config: FluvioClusterConfig::new("localhost:9003".to_string()),
                next_offset_to_read: None,
                consumer_config: ConsumerConfigExt::builder()
                    .topic("test_topic".to_string())
                    .offset_start(Offset::beginning())
                    .disable_continuous(true)
                    .offset_strategy(OffsetManagementStrategy::Manual)
                    .offset_consumer("test_consumer".to_string())
                    .build()
                    .expect("no error"),
            },
            state: ConsumerRetryState::Idle,
            stream: Some(Box::pin(multi_stream)),
            notify: Arc::new(Notify::new()),
        };

        //when
        let mut result = vec![];
        assert!(matches!(retry_stream.state, ConsumerRetryState::Idle));
        let next = retry_stream.next().await.unwrap().unwrap();
        result.push(next);

        //then
        assert!(matches!(retry_stream.state, ConsumerRetryState::Idle));
        while let Some(r) = retry_stream.next().await {
            result.push(r.unwrap());
        }

        assert_eq!(
            result
                .iter()
                .map(|r| String::from_utf8_lossy(r.as_ref()).to_string())
                .collect::<Vec<_>>(),
            ["1", "2", "3", "4", "5", "6"]
        );
        assert!(matches!(retry_stream.state, ConsumerRetryState::Terminated));

        retry_stream.offset_commit().await.unwrap();
        fluvio_future::task::spawn(async move {
            let message = rx1.recv().await;
            if let Ok(StreamToServer::FlushManagedOffset {
                offset: _,
                callback,
            }) = message
            {
                callback.send(ErrorCode::None).await;
            }
        });
        fluvio_future::task::spawn(async move {
            let message = rx2.recv().await;
            if let Ok(StreamToServer::FlushManagedOffset {
                callback,
                offset: _,
            }) = message
            {
                callback.send(ErrorCode::None).await;
            }
        });

        assert!(retry_stream.offset_flush().await.is_ok())
    }

    fn records_stream(
        partition: PartitionId,
        input: impl IntoIterator<Item = &'static str>,
    ) -> Iter<IntoIter<Result<ConsumerRecord, ErrorCode>>> {
        let mut records: Vec<_> = input
            .into_iter()
            .map(|item| fluvio_protocol::record::Record::new(RecordData::from(item.as_bytes())))
            .collect();
        let mut batch = Batch::default();
        batch.add_records(&mut records);
        let consumer_records: Vec<_> = batch
            .into_consumer_records_iter(partition)
            .map(Ok)
            .collect();
        futures_util::stream::iter(consumer_records)
    }
}
