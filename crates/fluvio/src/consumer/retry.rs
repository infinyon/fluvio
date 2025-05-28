use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;
use adaptive_backoff::prelude::{
    Backoff, BackoffBuilder, ExponentialBackoff, ExponentialBackoffBuilder,
};
use async_lock::Mutex;
use async_trait::async_trait;
use fluvio_socket::ClientConfig;
use fluvio_types::defaults::{
    RECONNECT_BACKOFF_FACTOR, RECONNECT_BACKOFF_MAX_DURATION, RECONNECT_BACKOFF_MIN_DURATION,
};
use futures_util::Stream;
use futures_util::StreamExt;
use tokio::select;
use tracing::{debug, info, warn};

use fluvio_future::timer::sleep;
use fluvio_protocol::record::ConsumerRecord;
use fluvio_sc_schema::errors::ErrorCode;

use crate::consumer::RetryMode;
use crate::{Fluvio, FluvioClusterConfig, Offset};
use super::{
    BoxConsumerFuture, BoxConsumerStream, ConsumerBoxFuture, ConsumerConfigExt,
    ConsumerFutureOutput, ConsumerStream, ShararedConsumerStream,
};

pub const SPAN_RETRY: &str = "fluvio::retry";

/// A consumer stream that automatically retries on failure.
///
/// In this refactored version we remove the mutex by taking ownership of
/// the consumer stream whenever we start a new retry task. When the task finishes,
/// the stream is returned.
pub struct ConsumerRetryStream {
    inner: ConsumerRetryInner,
    state: ConsumerRetryState,
    stream: ShararedConsumerStream,
}

impl ConsumerRetryStream {
    fn change_state(&mut self, new_state: ConsumerRetryState) {
        self.state = new_state;
    }

    fn set_idle(&mut self) {
        self.change_state(ConsumerRetryState::Idle);
    }

    fn set_terminated(&mut self) {
        self.change_state(ConsumerRetryState::Terminated);
    }

    fn set_task(&mut self, task: BoxConsumerFuture) {
        self.change_state(ConsumerRetryState::Task(task));
    }
}

#[derive(Clone)]
pub struct ConsumerRetryInner {
    cluster_config: FluvioClusterConfig,
    next_offset_to_read: Option<i64>,
    consumer_config: ConsumerConfigExt,
    client_config: Arc<ClientConfig>,
    strategy: Arc<dyn ReconnectStrategy>,
    backoff: ExponentialBackoff,
}
impl ConsumerRetryInner {
    /// Determine the offset for reconnection.
    fn next_offset(&self) -> Offset {
        if let Some(next) = self.next_offset_to_read {
            Offset::absolute(next).unwrap_or_else(|_| self.consumer_config.offset_start.clone())
        } else {
            self.consumer_config.offset_start.clone()
        }
    }
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

/// A trait for retry strategies.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(test, mockall::automock)]
pub trait ReconnectStrategy: Send + Sync {
    /// Retry strategy implementation.
    async fn reconnect(
        &self,
        inner: &ConsumerRetryInner,
        new_config: ConsumerConfigExt,
        backoff: ExponentialBackoff,
    ) -> Result<Arc<async_lock::Mutex<BoxConsumerStream>>>;
}

/// A default reconnect strategy that uses the Fluvio client to reconnect.
pub struct DefaultReconnectStrategy;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ReconnectStrategy for DefaultReconnectStrategy {
    async fn reconnect(
        &self,
        inner: &ConsumerRetryInner,
        new_config: ConsumerConfigExt,
        mut backoff: ExponentialBackoff,
    ) -> Result<Arc<async_lock::Mutex<BoxConsumerStream>>> {
        info!(target: SPAN_RETRY, "Reconnecting to stream consumer");
        let fluvio_client = Fluvio::connect_with_connector(
            inner.client_config.connector().clone(),
            &inner.cluster_config,
        )
        .await?;

        let new_stream =
            ConsumerRetryStream::create_owned_stream(fluvio_client, new_config.clone()).await?;

        backoff.reset();
        Ok(Arc::new(Mutex::new(Box::pin(new_stream))))
    }
}

impl Stream for ConsumerRetryStream {
    type Item = Result<ConsumerRecord, ErrorCode>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                ConsumerRetryState::Terminated => return Poll::Ready(None),
                ConsumerRetryState::Idle => {
                    // Start a new retry task.
                    let future = Self::consumer_with_retry(self.inner.clone(), self.stream.clone());
                    self.set_task(Box::pin(future));
                }
                ConsumerRetryState::Task(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready((new_stream, opt_result)) => {
                        self.stream = new_stream;
                        self.set_idle();
                        match opt_result {
                            Some(Ok((record, new_offset))) => {
                                self.inner.next_offset_to_read = new_offset;
                                return Poll::Ready(Some(Ok(record)));
                            }
                            Some(Err(e)) => {
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
        Box::pin(async move {
            let mut stream = self.stream.lock().await;
            stream.offset_commit().await
        })
    }

    fn offset_flush(&mut self) -> ConsumerBoxFuture {
        Box::pin(async move {
            let mut stream = self.stream.lock().await;
            stream.offset_flush().await
        })
    }
}

impl ConsumerRetryStream {
    /// Creates an owned consumer stream from a Fluvio client.
    async fn create_owned_stream(
        fluvio: Fluvio,
        config: ConsumerConfigExt,
    ) -> Result<
        impl ConsumerStream<
            Item = std::result::Result<ConsumerRecord, fluvio_protocol::link::ErrorCode>,
        >,
    > {
        fluvio.consumer_with_config_inner(config).await
    }

    /// Creates a new `ConsumerRetryStream` with the given configuration.
    pub async fn new(
        fluvio: &Fluvio,
        cluster_config: FluvioClusterConfig,
        config: ConsumerConfigExt,
    ) -> Result<Self> {
        let client_config = fluvio.client_config();
        let stream = fluvio.consumer_with_config_inner(config.clone()).await?;

        let backoff = create_backoff()?;

        let retry_stream = Self {
            inner: ConsumerRetryInner {
                client_config,
                cluster_config,
                next_offset_to_read: None,
                consumer_config: config,
                strategy: Arc::new(DefaultReconnectStrategy),
                backoff,
            },
            state: ConsumerRetryState::Idle,
            stream: Arc::new(Mutex::new(Box::pin(stream))),
        };

        Ok(retry_stream)
    }

    /// Consumes records from the stream with retry logic.
    ///
    /// On error or end-of-stream, it waits (using exponential backoff) and then
    /// reconnects. When a record is successfully produced, the new stream and
    /// updated offset are returned.
    async fn consumer_with_retry(
        mut inner: ConsumerRetryInner,
        mut stream: ShararedConsumerStream,
    ) -> ConsumerFutureOutput {
        let mut attempts: u32 = 0;
        let mut backoff = inner.backoff.clone();

        loop {
            // Attempt to consume records from the stream.
            if let Some(consumer) = Self::handle_consumer_records(&mut inner, stream.clone()).await
            {
                return consumer;
            }

            if inner.consumer_config.retry_mode == RetryMode::Disabled {
                return (stream.clone(), None);
            }

            // If continuous consumption is disabled, end the stream.
            if inner.consumer_config.disable_continuous {
                return (stream.clone(), None);
            }

            // Wait before retrying.
            backoff_and_wait(&mut backoff).await;
            attempts += 1;

            // Update the consumer configuration with the new offset.
            let mut new_config = inner.consumer_config.clone();
            new_config.offset_start = inner.next_offset();

            // Reconnect loop
            match Self::handle_reconnection_loop(&mut inner, backoff.clone(), attempts, new_config)
                .await
            {
                Ok(new_stream) => {
                    drop(stream); // Drop the old stream
                    stream = new_stream;
                }
                Err(err) => {
                    return (stream.clone(), Some(Err(err)));
                }
            }
        }
    }

    /// Handles the consumer records stream.
    async fn handle_consumer_records(
        inner: &mut ConsumerRetryInner,
        stream: ShararedConsumerStream,
    ) -> Option<ConsumerFutureOutput> {
        loop {
            let mut stream_lock = stream.lock().await;
            select! {
                _ = sleep(Duration::from_millis(100)) => {
                    // Timeout reached, release the lock
                    // to allow commit/flush operations.
                    drop(stream_lock);
                    continue;
                }
                consumer_opt = stream_lock.next() => {
                    // Try to retrieve the next record.
                    if let Some(record_result) = consumer_opt {
                        match record_result {
                            Ok(record) => {
                                let new_offset = Some(record.offset + 1);
                                return Some((stream.clone(), Some(Ok((record, new_offset)))));
                            }
                            Err(ErrorCode::OffsetEvicted {
                                next_available,
                                offset,
                            }) => {
                                info!(
                                    "Offset evicted: {}. Next available: {}",
                                    offset, next_available
                                );
                                warn!(target: SPAN_RETRY, "Offset evicted: {}. Next available: {}", offset, next_available);
                                inner.next_offset_to_read = Some(next_available);
                                if let RetryMode::Disabled = inner.consumer_config.retry_mode {
                                    return Some((
                                        stream.clone(),
                                        Some(Err(ErrorCode::OffsetEvicted {
                                            next_available,
                                            offset,
                                        })),
                                    ));
                                }
                            }
                            Err(e) => {
                                warn!(target: SPAN_RETRY, "Error consuming record: {}", e);
                                if let RetryMode::Disabled = inner.consumer_config.retry_mode {
                                    return Some((stream.clone(), Some(Err(e))));
                                }
                            }
                        }
                    }

                    return None;
                }
            }
        }
    }

    /// Keep trying until a new stream is created.
    async fn handle_reconnection_loop(
        inner: &mut ConsumerRetryInner,
        mut backoff: ExponentialBackoff,
        mut attempts: u32,
        new_config: ConsumerConfigExt,
    ) -> Result<ShararedConsumerStream, ErrorCode> {
        let offset_start = new_config.offset_start.clone();
        info!(target: SPAN_RETRY, ?offset_start, "Reconnecting to stream");
        loop {
            match inner
                .strategy
                .reconnect(inner, new_config.clone(), backoff.clone())
                .await
            {
                Ok(new_stream) => {
                    info!(target: SPAN_RETRY, ?offset_start, "Created new consumer stream");
                    return Ok(new_stream);
                }
                Err(e) => {
                    backoff_and_wait(&mut backoff).await;
                    attempts += 1;
                    warn!(target: SPAN_RETRY, "Could not connect to stream on {}: {}", inner.consumer_config.topic, e);

                    match inner.consumer_config.retry_mode {
                        RetryMode::TryUntil(max) if attempts >= max => {
                            return Err(ErrorCode::MaxRetryReached);
                        }
                        RetryMode::Disabled => {
                            return Err(ErrorCode::Other(format!("{}", e)));
                        }
                        _ => {
                            continue; // Retry
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
        .factor(RECONNECT_BACKOFF_FACTOR)
        .min(RECONNECT_BACKOFF_MIN_DURATION)
        .max(RECONNECT_BACKOFF_MAX_DURATION)
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
    use std::{sync::Arc, vec::IntoIter};

    use async_lock::Mutex;
    use fluvio_protocol::record::Batch;
    use fluvio_smartmodule::RecordData;
    use fluvio_types::PartitionId;
    use futures_util::{stream::Iter, FutureExt, StreamExt};

    use crate::consumer::{
        MultiplePartitionConsumerStream, OffsetManagementStrategy, SinglePartitionConsumerStream,
        StreamToServer,
    };

    use super::*;

    /// A reconnect strategy that always fails immediately.
    struct FailingReconnectStrategy;

    #[async_trait::async_trait]
    impl super::ReconnectStrategy for FailingReconnectStrategy {
        async fn reconnect(
            &self,
            _inner: &super::ConsumerRetryInner,
            _cfg: ConsumerConfigExt,
            _backoff: super::ExponentialBackoff,
        ) -> anyhow::Result<ShararedConsumerStream> {
            Err(anyhow::anyhow!("forced failure"))
        }
    }

    /// Build an `inner` & initial shared stream with a minimal configuration that can be
    /// tweaked per‑test.
    fn make_basic_single_stream<T>(
        input: T,
        strategy: Arc<dyn ReconnectStrategy>,
        retry_mode: RetryMode,
    ) -> ConsumerRetryStream
    where
        T: Stream<Item = Result<ConsumerRecord, ErrorCode>> + Send + 'static,
        T: std::marker::Unpin,
    {
        let partition_stream = SinglePartitionConsumerStream::new(
            input,
            OffsetManagementStrategy::Auto,
            Default::default(),
            Duration::from_millis(100),
            flume::unbounded::<crate::consumer::StreamToServer>().0,
        );
        let multi_stream = MultiplePartitionConsumerStream::new([partition_stream]);

        let inner = ConsumerRetryInner {
            client_config: Arc::new(ClientConfig::with_addr("localhost:9010".to_string())),
            cluster_config: super::FluvioClusterConfig::new("localhost:9003".to_string()),
            next_offset_to_read: None,
            consumer_config: ConsumerConfigExt::builder()
                .topic("topic".to_string())
                .offset_start(Offset::beginning())
                .offset_strategy(OffsetManagementStrategy::Auto)
                .offset_consumer("test_consumer".to_string())
                .retry_mode(retry_mode)
                .disable_continuous(false)
                .build()
                .unwrap(),
            strategy,
            backoff: super::create_backoff().unwrap(),
        };

        ConsumerRetryStream {
            inner: inner.clone(),
            state: ConsumerRetryState::Idle,
            stream: Arc::new(Mutex::new(Box::pin(multi_stream))),
        }
    }

    fn create_data(
        partition: PartitionId,
        input: impl IntoIterator<Item = &'static str>,
    ) -> Vec<Result<ConsumerRecord, ErrorCode>> {
        let mut records: Vec<_> = input
            .into_iter()
            .map(|item| fluvio_protocol::record::Record::new(RecordData::from(item.as_bytes())))
            .collect();
        let mut batch = Batch::default();
        batch.add_records(&mut records);
        batch
            .into_consumer_records_iter(partition)
            .map(Ok)
            .collect()
    }

    fn records_stream(
        partition: PartitionId,
        input: impl IntoIterator<Item = &'static str>,
    ) -> Iter<IntoIter<Result<ConsumerRecord, ErrorCode>>> {
        let consumer_records = create_data(partition, input);
        futures_util::stream::iter(consumer_records)
    }

    #[fluvio_future::test]
    async fn test_retry_stream() {
        //given
        let (tx1, rx1) = flume::unbounded();
        let partition_stream1 = SinglePartitionConsumerStream::new(
            records_stream(0, ["1", "3", "5"]),
            OffsetManagementStrategy::Manual,
            Default::default(),
            Duration::from_millis(100),
            tx1,
        );
        let (tx2, rx2) = flume::unbounded();
        let partition_stream2 = SinglePartitionConsumerStream::new(
            records_stream(1, ["2", "4", "6"]),
            OffsetManagementStrategy::Manual,
            Default::default(),
            Duration::from_millis(100),
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
                strategy: Arc::new(DefaultReconnectStrategy),
                backoff: ExponentialBackoff::default(),
            },
            state: ConsumerRetryState::Idle,
            stream: Arc::new(Mutex::new(Box::pin(multi_stream))),
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
            let message = rx1.recv_async().await;
            if let Ok(StreamToServer::FlushManagedOffset {
                offset: _,
                callback,
            }) = message
            {
                callback.send(ErrorCode::None).await;
            }
        });
        fluvio_future::task::spawn(async move {
            let message = rx2.recv_async().await;
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

    #[fluvio_future::test]
    async fn test_consumer_with_retry_handles_offset_evicted() {
        let mut consumer_records_with_error = create_data(0, ["1", "2"]);
        // given
        let eviction = ErrorCode::OffsetEvicted {
            next_available: 2,
            offset: 1,
        };
        consumer_records_with_error.push(Err(eviction.clone()));

        let mut mock = MockReconnectStrategy::new();

        mock.expect_reconnect()
            .withf(move |inner, new_config, _backoff| {
                assert!(!new_config.disable_continuous);
                assert_eq!(inner.consumer_config.topic, "topic");
                assert_eq!(inner.next_offset_to_read, Some(2));
                assert_eq!(new_config.offset_start, Offset::absolute(2).unwrap());
                assert_eq!(new_config.offset_strategy, OffsetManagementStrategy::Auto);
                assert_eq!(
                    new_config.offset_consumer,
                    Some("test_consumer".to_string())
                );
                true
            })
            .returning(move |_, _, _| {
                let retry_stream = make_basic_single_stream(
                    futures_util::stream::iter(create_data(0, ["7", "8", "9"])),
                    Arc::new(DefaultReconnectStrategy),
                    RetryMode::Disabled,
                );

                futures_util::future::ready(Ok(retry_stream.stream)).boxed()
            });

        let mut retry_stream = make_basic_single_stream(
            futures_util::stream::iter(consumer_records_with_error),
            Arc::new(mock),
            RetryMode::TryForever,
        );

        // when
        let mut result = vec![];
        for _ in 0..5 {
            assert!(matches!(retry_stream.state, ConsumerRetryState::Idle));
            let next = retry_stream.next().await.unwrap().unwrap();
            result.push(next);
        }

        // then
        assert_eq!(
            result
                .iter()
                .map(|r| String::from_utf8_lossy(r.as_ref()).to_string())
                .collect::<Vec<_>>(),
            ["1", "2", "7", "8", "9"]
        );
        assert!(matches!(retry_stream.state, ConsumerRetryState::Idle));
    }

    #[fluvio_future::test]
    async fn retry_mode_disabled_ends_with_error() {
        // given
        let err = ErrorCode::Other("broken".into());
        let mut retry_stream = make_basic_single_stream(
            futures_util::stream::iter(vec![Err(err.clone())]),
            Arc::new(FailingReconnectStrategy),
            RetryMode::Disabled,
        );

        // when
        let got = retry_stream.next().await.unwrap();
        assert_eq!(got.err().unwrap(), err);

        // then
        assert!(retry_stream.next().await.is_none());
        assert!(matches!(retry_stream.state, ConsumerRetryState::Terminated));
    }

    #[fluvio_future::test]
    async fn try_until_stops_after_max_attempts() {
        // given
        const ATTEMPTS: u32 = 3;
        let mut retry_stream = make_basic_single_stream(
            records_stream(0, ["a"]),
            Arc::new(FailingReconnectStrategy),
            RetryMode::TryUntil(ATTEMPTS),
        );

        // when
        assert!(retry_stream.next().await.unwrap().is_ok());
        let result = retry_stream.next().await.unwrap();

        // then
        assert_eq!(result.err().unwrap(), ErrorCode::MaxRetryReached);
    }
}
