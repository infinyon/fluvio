use std::future::Future;
use std::task::{Context, Poll};
use std::{pin::Pin, sync::Arc};
use std::time::Duration;

use anyhow::Result;
use fluvio_protocol::record::ConsumerRecord;
use fluvio_spu_schema::Isolation;
use futures_util::stream::Stream;
use futures_util::StreamExt;
use tracing::{debug, error, info, warn};

use fluvio_sc_schema::errors::ErrorCode;
use crate::{Fluvio, FluvioConfig, Offset};
use super::{ConsumerConfigExt, ConsumerStream};

pub const MAX_RETRY_PAIRS: u32 = 20;
pub const RETRY_BACKOFF: Duration = Duration::from_secs(1);

// Define type aliases that conditionally include +Send for non-wasm targets.
#[cfg(target_arch = "wasm32")]
type BoxConsumerStream =
    Pin<Box<dyn ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + 'static>>;
#[cfg(not(target_arch = "wasm32"))]
type BoxConsumerStream =
    Pin<Box<dyn ConsumerStream<Item = Result<ConsumerRecord, ErrorCode>> + Send + 'static>>;

#[cfg(target_arch = "wasm32")]
type BoxPendingFuture =
    Pin<Box<dyn Future<Output = (ConsumerRecord, BoxConsumerStream, Option<i64>)> + 'static>>;
#[cfg(not(target_arch = "wasm32"))]
type BoxPendingFuture = Pin<
    Box<dyn Future<Output = (ConsumerRecord, BoxConsumerStream, Option<i64>)> + Send + 'static>,
>;

pub struct ConsumerWithRetry {
    fluvio_client: Arc<Fluvio>,
    next_offset_to_read: Option<i64>,
    start_offset: Offset,
    current_stream: Option<BoxConsumerStream>,
    max_retry_pairs: u32,
    retry_backoff: Duration,
    hard_exit_on_fail: bool,
    pending: Option<BoxPendingFuture>,
    topic_name: String,
    config: ConsumerConfigExt,
}

impl Stream for ConsumerWithRetry {
    type Item = Result<ConsumerRecord, ErrorCode>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // If there is no pending future, create one.
        if this.pending.is_none() {
            // Take the current stream out so we can pass ownership to the async helper.
            let current_stream = this
                .current_stream
                .take()
                .expect("current_stream must be available");
            let next_offset = this.next_offset_to_read;
            let topic_name = this.topic_name.clone();
            let config = this.config.clone();
            let fluvio_client = Arc::clone(&this.fluvio_client);
            let max_retry_pairs = this.max_retry_pairs;
            let retry_backoff = this.retry_backoff;
            let hard_exit_on_fail = this.hard_exit_on_fail;
            let start_offset = this.start_offset.clone();

            this.pending = Some(Box::pin(ConsumerWithRetry::consume_with_retry_owned(
                fluvio_client,
                current_stream,
                next_offset,
                start_offset,
                topic_name,
                config,
                max_retry_pairs,
                retry_backoff,
                hard_exit_on_fail,
            )));
        }

        // Poll the pending future.
        if let Some(ref mut fut) = this.pending {
            match fut.as_mut().poll(cx) {
                Poll::Ready((record, updated_stream, updated_offset)) => {
                    // Save the updated state.
                    this.current_stream = Some(updated_stream);
                    this.next_offset_to_read = updated_offset;
                    this.pending = None;
                    Poll::Ready(Some(Ok(record)))
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
            Err(ErrorCode::Other("No current stream".into()))
        }
    }

    fn offset_flush(
        &mut self,
    ) -> futures_util::future::BoxFuture<'_, std::result::Result<(), ErrorCode>> {
        if let Some(ref mut stream) = self.current_stream {
            stream.offset_flush()
        } else {
            Box::pin(async { Err(ErrorCode::Other("No current stream".into())) })
        }
    }
}

impl ConsumerWithRetry {
    pub async fn new(
        fluvio_config: FluvioConfig,
        topic_name: String,
        max_retry_pairs: u32,
        retry_backoff: Duration,
        hard_exit_on_fail: bool,
        start_offset: Offset,
    ) -> Result<Self> {
        let fluvio = Fluvio::connect_with_config(&fluvio_config).await?;
        let config = ConsumerConfigExt::builder()
            .topic(&topic_name)
            .isolation(Isolation::ReadCommitted)
            .offset_start(start_offset.clone())
            .build()?;
        let stream = fluvio.consumer_with_config_inner(config.clone()).await?;
        // Coerce the concrete stream into our trait object.
        let current_stream: BoxConsumerStream = Box::pin(stream);
        let current_stream = Some(current_stream);

        Ok(Self {
            fluvio_client: Arc::new(fluvio),
            next_offset_to_read: None,
            start_offset,
            current_stream,
            pending: None,
            max_retry_pairs,
            retry_backoff,
            hard_exit_on_fail,
            topic_name,
            config,
        })
    }

    pub async fn get_last_known_offset(&mut self) -> Result<Option<u64>> {
        let offset = Offset::from_end(1);
        let config = ConsumerConfigExt::builder()
            .topic(&self.topic_name)
            .offset_start(offset)
            .disable_continuous(true)
            .build()?;
        let mut stream = self
            .fluvio_client
            .consumer_with_config_inner(config)
            .await?;
        let last_known = stream.next().await.transpose()?.map(|r| r.offset());
        let last_known: Option<u64> = last_known.map(|f| f.try_into()).transpose()?;
        Ok(last_known)
    }

    // This method takes ownership of the current_stream temporarily,
    // calls the owned helper, and then restores state.
    pub async fn consume_with_retry(&mut self) -> ConsumerRecord {
        let current_stream = self
            .current_stream
            .take()
            .expect("current_stream must be available");
        let next_offset = self.next_offset_to_read;
        let topic_name = self.topic_name.clone();
        let config = self.config.clone();
        let fluvio_client = Arc::clone(&self.fluvio_client);
        let max_retry_pairs = self.max_retry_pairs;
        let retry_backoff = self.retry_backoff;
        let hard_exit_on_fail = self.hard_exit_on_fail;
        let start_offset = self.start_offset.clone();

        let (record, updated_stream, updated_offset) = ConsumerWithRetry::consume_with_retry_owned(
            fluvio_client,
            current_stream,
            next_offset,
            start_offset,
            topic_name,
            config,
            max_retry_pairs,
            retry_backoff,
            hard_exit_on_fail,
        )
        .await;

        self.current_stream = Some(updated_stream);
        self.next_offset_to_read = updated_offset;
        record
    }

    // The refactored async helper that performs the consumption logic without borrowing self.
    async fn consume_with_retry_owned(
        fluvio_client: Arc<Fluvio>,
        mut current_stream: BoxConsumerStream,
        next_offset: Option<i64>,
        start_offset: Offset,
        topic_name: String,
        config: ConsumerConfigExt,
        max_retry_pairs: u32,
        retry_backoff: Duration,
        hard_exit_on_fail: bool,
    ) -> (ConsumerRecord, BoxConsumerStream, Option<i64>) {
        let mut i = 0;
        loop {
            // Try to get a record from the current stream.
            if let Some(record_result) = current_stream.as_mut().next().await {
                match record_result {
                    Ok(record) => {
                        let new_offset = Some(record.offset + 1);
                        if i > 0 {
                            debug!("Record produced successfully after reconnect");
                        }
                        return (record, current_stream, new_offset);
                    }
                    Err(e) => {
                        warn!("Failed to consume from stream on {}: {}", topic_name, e);
                    }
                }
            }
            fluvio_future::timer::sleep(retry_backoff).await;
            // Attempt to reconnect to the stream.
            fluvio_future::timer::sleep(retry_backoff).await;
            let offset = if let Some(next) = next_offset {
                Offset::absolute(next).expect("Unable to create offset")
            } else {
                start_offset.clone()
            };
            let mut new_config = config.clone();
            new_config.offset_start = offset;
            match fluvio_client.consumer_with_config(new_config).await {
                Ok(new_stream) => {
                    // Coerce new_stream into our trait object.
                    current_stream = Box::pin(new_stream) as BoxConsumerStream;
                    info!("Created new consume stream with offset: {:?}", next_offset);
                    continue;
                }
                Err(e) => {
                    error!("Could not connect to stream on {}: {}", topic_name, e);
                }
            }
            // Attempt to reconnect to the consumer.
            fluvio_future::timer::sleep(retry_backoff).await;
            i += 1;
            while fluvio_client
                .consumer_with_config(config.clone())
                .await
                .is_err()
                && i < max_retry_pairs
            {
                fluvio_future::timer::sleep(retry_backoff).await;
                i += 1;
            }
            if i >= max_retry_pairs {
                break;
            }
        }
        if hard_exit_on_fail {
            error!("Exiting due to irrecoverable failure to consume");
            std::process::exit(1);
        } else {
            error!("Panicking due to irrecoverable failure to consume");
            panic!("Failed to consume");
        }
    }
}
