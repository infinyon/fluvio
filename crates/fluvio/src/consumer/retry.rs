use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;
use adaptive_backoff::prelude::{
    Backoff, BackoffBuilder, ExponentialBackoff, ExponentialBackoffBuilder,
};
use futures_util::future::BoxFuture;
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use fluvio_future::{task::run_block_on, timer::sleep};
use fluvio_protocol::record::ConsumerRecord;
use fluvio_sc_schema::errors::ErrorCode;

use crate::consumer::RetryMode;
use crate::{Fluvio, FluvioConfig, Offset};
use super::{ConsumerConfigExt, ConsumerStream};

pub const SPAN_RETRY: &str = "fluvio::retry";
pub const BACKOFF_MIN_DURATION: Duration = Duration::from_secs(1);
pub const BACKOFF_MAX_DURATION: Duration = Duration::from_secs(30);
pub const BACKOFF_FACTOR: f64 = 1.1;

/// Type alias for a consumer stream with conditional `Send` support.
#[cfg(target_arch = "wasm32")]
type BoxConsumerStream =
    Pin<Box<dyn ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + 'static>>;
#[cfg(not(target_arch = "wasm32"))]
type BoxConsumerStream =
    Pin<Box<dyn ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + Send + 'static>>;

/// Type alias for a pending future with conditional `Send` support.
#[cfg(target_arch = "wasm32")]
type BoxPendingFuture = Pin<
    Box<
        dyn Future<
                Output = Option<
                    Result<(ConsumerRecord, BoxConsumerStream, Option<i64>), ErrorCode>,
                >,
            > + 'static,
    >,
>;
#[cfg(not(target_arch = "wasm32"))]
type BoxPendingFuture = Pin<
    Box<
        dyn Future<
                Output = Option<
                    Result<(ConsumerRecord, BoxConsumerStream, Option<i64>), ErrorCode>,
                >,
            > + Send
            + 'static,
    >,
>;

#[derive(Clone)]
pub struct ConsumerWithRetryInner {
    fluvio_client: Arc<Fluvio>,
    next_offset_to_read: Option<i64>,
    consumer_config: ConsumerConfigExt,
}

/// The internal state of our consumer.
enum ConsumerState {
    /// An active stream is available.
    Current(BoxConsumerStream),
    /// A pending future is running (e.g. while reconnecting).
    Pending(BoxPendingFuture),
    /// The stream has terminated.
    Terminated,
}

/// A consumer stream that automatically retries on failure.
///
/// This version uses a state machine plus a notification primitive to signal
/// when the state changes. (See offset_flush and offset_commit below.)
pub struct ConsumerWithRetry {
    inner: ConsumerWithRetryInner,
    state: ConsumerState,
    notify: Arc<Notify>,
}

impl ConsumerWithRetry {
    fn change_state(&mut self, new_state: ConsumerState) {
        self.state = new_state;
        self.notify.notify_one();
    }
}

impl Stream for ConsumerWithRetry {
    type Item = Result<ConsumerRecord, ErrorCode>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // We use a loop so that after a state transition we immediately poll again.
        let this = self.get_mut();
        loop {
            match &mut this.state {
                ConsumerState::Terminated => return Poll::Ready(None),
                ConsumerState::Current(_) => {
                    // Transition into Pending so we can take ownership of the stream.
                    let stream = match std::mem::replace(&mut this.state, ConsumerState::Terminated)
                    {
                        ConsumerState::Current(s) => s,
                        _ => unreachable!(),
                    };

                    let client = Arc::clone(&this.inner.fluvio_client);
                    let new_state = ConsumerState::Pending(Box::pin(Self::consumer_with_retry(
                        this.inner.clone(),
                        client,
                        stream,
                    )));

                    this.change_state(new_state);
                    // Loop to poll the new pending future.
                }
                ConsumerState::Pending(pending) => {
                    match pending.as_mut().poll(cx) {
                        Poll::Ready(Some(Ok((record, new_stream, new_offset)))) => {
                            this.inner.next_offset_to_read = new_offset;
                            this.change_state(ConsumerState::Current(new_stream));
                            return Poll::Ready(Some(Ok(record)));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            // Transition back to Idle with a dummy stream.
                            this.notify.notify_one();
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            this.change_state(ConsumerState::Terminated);
                            return Poll::Ready(None);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl ConsumerStream for ConsumerWithRetry {
    /// TODO: rework this as an async method.
    fn offset_commit(&mut self) -> Result<(), ErrorCode> {
        loop {
            match &mut self.state {
                ConsumerState::Current(stream) => return stream.offset_commit(),
                ConsumerState::Terminated => {
                    warn!("offset_commit called but stream is terminated");
                    return Ok(());
                }
                _ => {
                    let notify = self.notify.clone();
                    run_block_on(async move {
                        notify.notified().await;
                    });
                }
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn offset_flush(&mut self) -> BoxFuture<'_, Result<(), ErrorCode>> {
        let notify = self.notify.clone();
        Box::pin(async move {
            loop {
                match self.state {
                    ConsumerState::Current(ref mut stream) => return stream.offset_flush().await,
                    ConsumerState::Terminated => {
                        warn!("offset_flush called but stream is terminated");
                        return Ok(());
                    }
                    _ => {
                        notify.notified().await;
                    }
                }
            }
        })
    }

    #[cfg(target_arch = "wasm32")]
    fn offset_flush(&mut self) -> BoxFuture<'_, Result<(), ErrorCode>> {
        loop {
            match self.state {
                ConsumerState::Current(ref mut stream) => return stream.offset_flush(),
                ConsumerState::Terminated => {
                    warn!("offset_flush called but stream is terminated");
                    return Box::pin(async { Ok(()) });
                }
                _ => {
                    let notify = self.notify.clone();
                    run_block_on(async move {
                        notify.notified().await;
                    });
                }
            }
        }
    }
}

impl ConsumerWithRetry {
    /// Creates a new `ConsumerWithRetry` instance.
    pub async fn new(fluvio_config: FluvioConfig, config: ConsumerConfigExt) -> Result<Self> {
        let fluvio = Fluvio::connect_with_config(&fluvio_config).await?;
        let stream = fluvio.consumer_with_config_inner(config.clone()).await?;
        let boxed_stream: BoxConsumerStream = Box::pin(stream);

        Ok(Self {
            inner: ConsumerWithRetryInner {
                fluvio_client: Arc::new(fluvio),
                next_offset_to_read: None,
                consumer_config: config,
            },
            state: ConsumerState::Current(boxed_stream),
            notify: Arc::new(Notify::new()),
        })
    }

    /// Consumes records from the stream with retry logic.
    ///
    /// On error or end-of-stream, it waits (using exponential backoff) and then
    /// reconnects. When a record is successfully produced, the new stream and
    /// updated offset are returned.
    async fn consumer_with_retry(
        inner: ConsumerWithRetryInner,
        fluvio_client: Arc<Fluvio>,
        mut current_stream: BoxConsumerStream,
    ) -> Option<Result<(ConsumerRecord, BoxConsumerStream, Option<i64>), ErrorCode>> {
        let mut attempts: u32 = 0;
        let mut backoff = match create_backoff() {
            Ok(b) => b,
            Err(_) => {
                return Some(Err(ErrorCode::Other("Error creating backoff".to_string())));
            }
        };

        loop {
            // Try to retrieve the next record.
            if let Some(record_result) = current_stream.as_mut().next().await {
                match record_result {
                    Ok(record) => {
                        let new_offset = Some(record.offset + 1);
                        if attempts > 0 {
                            debug!(target: SPAN_RETRY, "Record produced successfully after reconnect");
                        }
                        return Some(Ok((record, current_stream, new_offset)));
                    }
                    Err(e) => {
                        warn!(target: SPAN_RETRY, "Error consuming record: {}", e);
                        if let RetryMode::Disabled = inner.consumer_config.retry_mode {
                            return Some(Err(e));
                        }
                    }
                }
            }

            // If continuous consumption is disabled, end the stream.
            if inner.consumer_config.disable_continuous {
                return None;
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
                        return Some(Err(ErrorCode::OffsetOutOfRange));
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
                info!(target: SPAN_RETRY, "Reconnecting to stream");
                match fluvio_client
                    .consumer_with_config_inner(new_config.clone())
                    .await
                {
                    Ok(new_stream) => {
                        backoff.reset();
                        current_stream = Box::pin(new_stream) as BoxConsumerStream;
                        info!(target: SPAN_RETRY, "Created new consumer stream with offset: {:?}", inner.next_offset_to_read);
                        break;
                    }
                    Err(e) => {
                        backoff_and_wait(&mut backoff).await;
                        attempts += 1;
                        warn!(target: SPAN_RETRY, "Could not connect to stream on {}: {}", inner.consumer_config.topic, e);

                        match inner.consumer_config.retry_mode {
                            RetryMode::TryUntil(max) if attempts >= max => {
                                return Some(Err(ErrorCode::MaxRetryReached));
                            }
                            RetryMode::Disabled => {
                                return Some(Err(ErrorCode::Other(format!("{}", e))));
                            }
                            _ => {} // Continue retrying.
                        }
                    }
                }
            }
        }
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
