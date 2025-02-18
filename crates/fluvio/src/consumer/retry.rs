use std::future::Future;
use std::task::{Context, Poll};
use std::{pin::Pin, sync::Arc, time::Duration};

use anyhow::Result;
use adaptive_backoff::prelude::{
    ExponentialBackoff, ExponentialBackoffBuilder, Backoff, BackoffBuilder,
};
use futures_util::stream::Stream;
use futures_util::StreamExt;
use tracing::{debug, info, warn};

use fluvio_future::timer::sleep;
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

/// A consumer stream that automatically retries on failure.
pub struct ConsumerWithRetry {
    inner: ConsumerWithRetryInner,
    current_stream: Option<BoxConsumerStream>,
    pending: Option<BoxPendingFuture>,
}

impl Stream for ConsumerWithRetry {
    type Item = Result<ConsumerRecord, ErrorCode>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();

        // If there is no pending future, create one using the current stream.
        if self_mut.pending.is_none() {
            let current_stream = if let Some(stream) = self_mut.current_stream.take() {
                stream
            } else {
                return Poll::Ready(None);
            };

            let client = Arc::clone(&self_mut.inner.fluvio_client);
            self_mut.pending = Some(Box::pin(Self::consumer_with_retry(
                self_mut.inner.clone(),
                client,
                current_stream,
            )));
        }

        // Poll the pending future and update the internal state.
        if let Some(ref mut fut) = self_mut.pending {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Some(Ok((record, updated_stream, new_offset)))) => {
                    self_mut.current_stream = Some(updated_stream);
                    self_mut.inner.next_offset_to_read = new_offset;
                    self_mut.pending = None;
                    Poll::Ready(Some(Ok(record)))
                }
                Poll::Ready(Some(Err(e))) => {
                    self_mut.pending = None;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Ready(None) => {
                    self_mut.pending = None;
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl ConsumerStream for ConsumerWithRetry {
    fn offset_commit(&mut self) -> std::result::Result<(), ErrorCode> {
        if let Some(ref mut stream) = self.current_stream {
            stream.offset_commit()
        } else {
            warn!("Can't commit, no current stream");
            Err(ErrorCode::NoCurrentStream)
        }
    }

    fn offset_flush(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, std::result::Result<(), ErrorCode>> {
        if let Some(ref mut stream) = self.current_stream {
            stream.offset_flush()
        } else {
            warn!("Can't flush, no current stream");
            Box::pin(async { Err(ErrorCode::NoCurrentStream) })
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
            current_stream: Some(boxed_stream),
            pending: None,
        })
    }

    /// Consumes records from the stream with retry logic.
    ///
    /// This function continuously attempts to retrieve a record from the current stream.
    /// On failure (or end-of-stream), it waits using an exponential backoff strategy
    /// and then attempts to reconnect.
    async fn consumer_with_retry(
        inner: ConsumerWithRetryInner,
        fluvio_client: Arc<Fluvio>,
        mut current_stream: BoxConsumerStream,
    ) -> Option<Result<(ConsumerRecord, BoxConsumerStream, Option<i64>), ErrorCode>> {
        let mut attempts: u32 = 0;
        let mut backoff = if let Ok(backoff) = create_backoff() {
            backoff
        } else {
            return Some(Err(ErrorCode::Other("Error creating backoff".to_string())));
        };

        loop {
            // Try to retrieve the next record.
            if let Some(record_result) = current_stream.as_mut().next().await {
                match record_result {
                    Ok(record) => {
                        let new_offset = Some(record.offset + 1);
                        if attempts > 0 {
                            debug!(
                                target: SPAN_RETRY,
                                "Record produced successfully after reconnect"
                            );
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
                        info!(
                            target: SPAN_RETRY,
                            "Created new consume stream with offset: {:?}", inner.next_offset_to_read
                        );
                        break;
                    }
                    Err(e) => {
                        backoff_and_wait(&mut backoff).await;
                        attempts += 1;
                        warn!(
                            target: SPAN_RETRY,
                            "Could not connect to stream on {}: {}",
                            inner.consumer_config.topic,
                            e
                        );

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
    info!(
        target: SPAN_RETRY,
        seconds = wait_duration.as_secs(),
        "Starting backoff: sleeping for duration"
    );
    sleep(wait_duration).await;
    debug!(target: SPAN_RETRY, "Resuming after backoff");
}
