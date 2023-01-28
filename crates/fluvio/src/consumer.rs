use std::sync::Arc;

use fluvio_types::PartitionId;
use tracing::{debug, error, trace, instrument, info, warn};
use futures_util::stream::{Stream, select_all};
use once_cell::sync::Lazy;
use futures_util::future::{Either, err, join_all};
use futures_util::stream::{StreamExt, once, iter};
use futures_util::FutureExt;

use fluvio_types::defaults::{FLUVIO_CLIENT_MAX_FETCH_BYTES, FLUVIO_MAX_SIZE_TOPIC_NAME};
use fluvio_types::event::offsets::OffsetPublisher;
use fluvio_spu_schema::server::stream_fetch::{
    DefaultStreamFetchRequest, DefaultStreamFetchResponse, CHAIN_SMARTMODULE_API,
};
use fluvio_spu_schema::Isolation;
use fluvio_protocol::record::ReplicaKey;
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::record::Batch;

use crate::{FluvioError};
use crate::metrics::ClientMetrics;
use crate::offset::{Offset, fetch_offsets};
use crate::spu::{SpuDirectory, SpuPool};
use derive_builder::Builder;

pub use fluvio_protocol::record::ConsumerRecord as Record;
pub use fluvio_spu_schema::server::smartmodule::SmartModuleInvocation;
pub use fluvio_spu_schema::server::smartmodule::SmartModuleInvocationWasm;
pub use fluvio_spu_schema::server::smartmodule::SmartModuleKind;
pub use fluvio_spu_schema::server::smartmodule::SmartModuleContextData;
pub use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;

/// An interface for consuming events from a particular partition
///
///
///
/// [`Offset`]: struct.Offset.html
/// [`partition_consumer`]: struct.Fluvio.html#method.partition_consumer
/// [`Fluvio`]: struct.Fluvio.html
pub struct PartitionConsumer<P = SpuPool> {
    topic: String,
    partition: PartitionId,
    pool: Arc<P>,
    metrics: Arc<ClientMetrics>,
}

impl<P> PartitionConsumer<P>
where
    P: SpuDirectory,
{
    pub fn new(
        topic: String,
        partition: PartitionId,
        pool: Arc<P>,
        metrics: Arc<ClientMetrics>,
    ) -> Self {
        Self {
            topic,
            partition,
            pool,
            metrics,
        }
    }

    /// Returns the name of the Topic that this consumer reads from
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the ID of the partition that this consumer reads from
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Return a shared instance of `ClientMetrics`
    pub fn metrics(&self) -> Arc<ClientMetrics> {
        self.metrics.clone()
    }

    /// Continuously streams events from a particular offset in the consumer's partition
    ///
    /// Streaming is one of the two ways to consume events in Fluvio.
    /// It is a continuous request for new records arriving in a partition,
    /// beginning at a particular offset. You specify the starting point of the
    /// stream using an [`Offset`] and periodically receive events, either individually
    /// or in batches.
    ///
    /// If you want more fine-grained control over how records are streamed,
    /// check out the [`stream_with_config`] method.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{PartitionConsumer, FluvioError};
    /// # use fluvio::{Offset, ConsumerConfig};
    /// # mod futures {
    /// #     pub use futures_util::stream::StreamExt;
    /// # }
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// use futures::StreamExt;
    /// let mut stream = consumer.stream(Offset::beginning()).await?;
    /// while let Some(Ok(record)) = stream.next().await {
    ///     let key = record.key().map(|key| String::from_utf8_lossy(key).to_string());
    ///     let value = String::from_utf8_lossy(record.value()).to_string();
    ///     println!("Got event: key={:?}, value={}", key, value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Offset`]: struct.Offset.html
    /// [`ConsumerConfig`]: struct.ConsumerConfig.html
    /// [`stream_with_config`]: struct.ConsumerConfig.html#method.stream_with_config
    #[instrument(skip(self, offset))]
    pub async fn stream(
        &self,
        offset: Offset,
    ) -> Result<impl Stream<Item = Result<Record, ErrorCode>>, FluvioError> {
        let config = ConsumerConfig::builder().build()?;
        let stream = self.stream_with_config(offset, config).await?;

        Ok(stream)
    }

    /// Continuously streams events from a particular offset in the consumer's partition
    ///
    /// Most of the time, you shouldn't need to use a custom [`ConsumerConfig`].
    /// If you don't know what these settings do, try checking out the simpler
    /// [`PartitionConsumer::stream`] method that uses the default streaming settings.
    ///
    /// Streaming is one of the two ways to consume events in Fluvio.
    /// It is a continuous request for new records arriving in a partition,
    /// beginning at a particular offset. You specify the starting point of the
    /// stream using an [`Offset`] and a [`ConsumerConfig`], and periodically
    /// receive events, either individually or in batches.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{PartitionConsumer, FluvioError};
    /// # use fluvio::{Offset, ConsumerConfig};
    /// # mod futures {
    /// #     pub use futures_util::stream::StreamExt;
    /// # }
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// use futures::StreamExt;
    /// // Use a custom max_bytes value in the config
    /// let fetch_config = ConsumerConfig::builder()
    ///     .max_bytes(1000)
    ///     .build()?;
    /// let mut stream = consumer.stream_with_config(Offset::beginning(), fetch_config).await?;
    /// while let Some(Ok(record)) = stream.next().await {
    ///     let key: Option<String> = record.key().map(|key| String::from_utf8_lossy(key).to_string());
    ///     let value = String::from_utf8_lossy(record.value());
    ///     println!("Got record: key={:?}, value={}", key, value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Offset`]: struct.Offset.html
    /// [`ConsumerConfig`]: struct.ConsumerConfig.html
    #[instrument(skip(self, offset, config))]
    pub async fn stream_with_config(
        &self,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<impl Stream<Item = Result<Record, ErrorCode>>, FluvioError> {
        let (stream, start_offset) = self
            .inner_stream_batches_with_config(offset, config)
            .await?;
        let partition = self.partition;
        let flattened = stream.flat_map(move |result: Result<Batch, _>| match result {
            Err(e) => Either::Right(once(err(e))),
            Ok(batch) => {
                let records =
                    batch
                        .into_consumer_records_iter(partition)
                        .filter_map(move |record| {
                            if record.offset >= start_offset {
                                Some(Ok(record))
                            } else {
                                None
                            }
                        });
                Either::Left(iter(records))
            }
        });

        Ok(flattened)
    }

    /// Continuously streams batches of messages, starting an offset in the consumer's partition
    ///
    /// ```
    /// # use fluvio::{PartitionConsumer, FluvioError};
    /// # use fluvio::{Offset, ConsumerConfig};
    /// # mod futures {
    /// #     pub use futures_util::stream::StreamExt;
    /// # }
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// use futures::StreamExt;
    /// // Use a custom max_bytes value in the config
    /// let fetch_config = ConsumerConfig::builder()
    ///     .max_bytes(1000)
    ///     .build()?;
    /// let mut stream = consumer.stream_batches_with_config(Offset::beginning(), fetch_config).await?;
    /// while let Some(Ok(batch)) = stream.next().await {
    ///     for record in batch.records() {
    ///         let key = record.key.as_ref().map(|key| String::from_utf8_lossy(key.as_ref()).to_string());
    ///         let value = String::from_utf8_lossy(record.value.as_ref()).to_string();
    ///         println!("Got record: key={:?}, value={}", key, value);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(skip(self, offset, config))]
    pub async fn stream_batches_with_config(
        &self,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<impl Stream<Item = Result<Batch, ErrorCode>>, FluvioError> {
        let (stream, _start_offset) = self
            .inner_stream_batches_with_config(offset, config)
            .await?;
        Ok(stream)
    }

    /// Continuously streams batches of messages, starting an offset in the consumer's partition
    /// Returns both the stream and the start offset of the stream.
    #[instrument(skip(self, offset, config))]
    async fn inner_stream_batches_with_config(
        &self,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<
        (
            impl Stream<Item = Result<Batch, ErrorCode>>,
            fluvio_protocol::record::Offset,
        ),
        FluvioError,
    > {
        let (stream, start_offset) = self.request_stream(offset, config).await?;
        let metrics = self.metrics.clone();
        let flattened =
            stream.flat_map(move |batch_result: Result<DefaultStreamFetchResponse, _>| {
                let response = match batch_result {
                    Ok(response) => response,
                    Err(e) => return Either::Right(once(err(e))),
                };

                // If we ever get an error_code AND batches of records, we want to first send
                // the records down the consumer stream, THEN an Err with the error inside.
                // This way the consumer always gets to read all records that were properly
                // processed before hitting an error, so that the error does not obscure those records.

                let inner_metrics = metrics.clone();
                let batches =
                    response
                        .partition
                        .records
                        .batches
                        .into_iter()
                        .map(move |raw_batch| {
                            inner_metrics
                                .consumer()
                                .add_records(raw_batch.records_len() as u64);
                            inner_metrics
                                .consumer()
                                .add_bytes(raw_batch.batch_len() as u64);

                            let batch: Result<Batch, _> = raw_batch.try_into();
                            match batch {
                                Ok(batch) => Ok(batch),
                                Err(err) => Err(ErrorCode::Other(err.to_string())),
                            }
                        });
                let error = {
                    let code = response.partition.error_code;
                    match code {
                        ErrorCode::None => None,
                        _ => Some(Err(code)),
                    }
                };

                let items = batches.chain(error.into_iter());
                Either::Left(iter(items))
            });

        Ok((flattened, start_offset))
    }

    /// Creates a stream of `DefaultStreamFetchResponse` for older consumers who rely
    /// on the internal structure of the fetch response. New clients should use the
    /// `stream` and `stream_with_config` methods.
    /// Returns both the stream and the start offset of the stream.
    #[instrument(skip(self, config))]
    async fn request_stream(
        &self,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<
        (
            impl Stream<Item = Result<DefaultStreamFetchResponse, ErrorCode>>,
            fluvio_protocol::record::Offset,
        ),
        FluvioError,
    > {
        use fluvio_future::task::spawn;
        use futures_util::stream::empty;

        let replica = ReplicaKey::new(&self.topic, self.partition);
        let mut serial_socket = self.pool.create_serial_socket(&replica).await?;
        let offsets = fetch_offsets(&mut serial_socket, &replica).await?;

        let start_absolute_offset = offset.resolve(&offsets).await?;
        let end_absolute_offset = offsets.last_stable_offset;
        let record_count = end_absolute_offset - start_absolute_offset;

        debug!(start_absolute_offset, end_absolute_offset, record_count);

        let mut stream_request = DefaultStreamFetchRequest {
            topic: self.topic.to_owned(),
            partition: self.partition,
            fetch_offset: start_absolute_offset,
            isolation: config.isolation,
            max_bytes: config.max_bytes,
            smartmodules: config.smartmodule,
            ..Default::default()
        };

        let stream_fetch_version = serial_socket
            .versions()
            .lookup_version::<DefaultStreamFetchRequest>()
            .unwrap_or(CHAIN_SMARTMODULE_API - 1);
        debug!(%stream_fetch_version, "stream_fetch_version");
        if stream_fetch_version < CHAIN_SMARTMODULE_API {
            warn!("SPU does not support SmartModule chaining. SmartModules will not be applied to the stream");
        }

        stream_request.derivedstream = config.derivedstream;

        let mut stream = self
            .pool
            .create_stream_with_version(&replica, stream_request, stream_fetch_version)
            .await?;

        let ft_stream = async move {
            if let Some(Ok(raw_response)) = stream.next().await {
                let response: DefaultStreamFetchResponse = raw_response;

                let stream_id = response.stream_id;

                trace!("first stream response: {:#?}", response);
                debug!(
                    stream_id,
                    last_offset = ?response.partition.next_offset_for_fetch(),
                    "first stream response"
                );

                let publisher = OffsetPublisher::shared(0);
                let mut listener = publisher.change_listener();

                // update stream with received offsets
                spawn(async move {
                    use fluvio_spu_schema::server::update_offset::{UpdateOffsetsRequest, OffsetUpdate};

                    loop {
                        let fetch_last_value = listener.listen().await;
                        debug!(fetch_last_value, stream_id, "received end fetch");
                        if fetch_last_value < 0 {
                            info!("fetch last is end, terminating");
                            break;
                        } else {
                            debug!(
                                offset = fetch_last_value,
                                session_id = stream_id,
                                "sending back offset to spu"
                            );
                            let request = UpdateOffsetsRequest {
                                offsets: vec![OffsetUpdate {
                                    offset: fetch_last_value,
                                    session_id: stream_id,
                                }],
                            };
                            debug!(?request, "Sending offset update request:");
                            let response = serial_socket.send_receive(request).await;
                            if let Err(err) = response {
                                error!("error sending offset: {:#?}", err);
                                break;
                            }
                        }
                    }
                    debug!(stream_id, "offset fetch update loop end");
                });

                // send back first offset records exists
                if let Some(last_offset) = response.partition.next_offset_for_fetch() {
                    debug!(last_offset, "notify new last offset");
                    publisher.update(last_offset);
                }

                let response_publisher = publisher.clone();
                let update_stream = StreamExt::map(stream, move |item| {
                    item.map(|response| {
                        if let Some(last_offset) = response.partition.next_offset_for_fetch() {
                            debug!(last_offset, stream_id, "received last offset from spu");
                            response_publisher.update(last_offset);
                        }
                        response
                    })
                    .map_err(|e| {
                        error!(?e, "error in stream");
                        ErrorCode::Other(e.to_string())
                    })
                });
                Either::Left(
                    iter(vec![Ok(response)])
                        .chain(publish_stream::EndPublishSt::new(update_stream, publisher)),
                )
            } else {
                info!("stream ended");
                Either::Right(empty())
            }
        };

        let stream = if config.disable_continuous {
            TakeRecords::new(ft_stream.flatten_stream().boxed(), record_count).boxed()
        } else {
            ft_stream.flatten_stream().boxed()
        };

        Ok((stream, start_absolute_offset))
    }
}

/// Wrap an inner record stream and only stream until a given number of records have been fetched.
///
/// This is used for "disable continuous" mode. In this mode, we first make a FetchOffsetPartitionResponse
/// in order to see the starting and ending offsets currently available for this partition.
/// Based on the starting offset the caller asks for, we can figure out the "record count", or
/// how many records from the start onward we know for sure we can stream without waiting.
/// We then use `TakeRecords` to stop the stream as soon as we reach that point, so the user
/// (e.g. on the CLI) does not spend any time waiting for new records to be produced, they are
/// simply given all the records that are already available.
struct TakeRecords<S> {
    remaining: i64,
    stream: S,
}

impl<S> TakeRecords<S>
where
    S: Stream<Item = Result<DefaultStreamFetchResponse, ErrorCode>> + std::marker::Unpin,
{
    pub fn new(stream: S, until: i64) -> Self {
        Self {
            remaining: until,
            stream,
        }
    }
}

impl<S> Stream for TakeRecords<S>
where
    S: Stream<Item = Result<DefaultStreamFetchResponse, ErrorCode>> + std::marker::Unpin,
{
    type Item = S::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::{pin::Pin, task::Poll};
        use futures_util::ready;
        if self.remaining <= 0 {
            return Poll::Ready(None);
        }
        let next = ready!(Pin::new(&mut self.as_mut().stream).poll_next(cx));
        match next {
            Some(Ok(response)) => {
                // Count how many records are present in this batch's response
                let count: usize = response
                    .partition
                    .records
                    .batches
                    .iter()
                    .map(|it| it.records_len())
                    .sum();
                let diff = self.remaining - count as i64;
                self.remaining = diff.max(0);
                Poll::Ready(Some(Ok(response)))
            }
            other => Poll::Ready(other),
        }
    }
}

mod publish_stream {

    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Poll, Context};

    use pin_project_lite::pin_project;
    use futures_util::ready;

    use super::Stream;
    use super::OffsetPublisher;

    // signal offset when stream is done
    pin_project! {
        pub struct EndPublishSt<St> {
            #[pin]
            stream: St,
            publisher: Arc<OffsetPublisher>
        }
    }

    impl<St> EndPublishSt<St> {
        pub fn new(stream: St, publisher: Arc<OffsetPublisher>) -> Self {
            Self { stream, publisher }
        }
    }

    impl<S: Stream> Stream for EndPublishSt<S> {
        type Item = S::Item;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
            let this = self.project();

            let item = ready!(this.stream.poll_next(cx));
            if item.is_none() {
                this.publisher.update(-1);
            }
            Poll::Ready(item)
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.stream.size_hint()
        }
    }
}

/// MAX FETCH BYTES
static MAX_FETCH_BYTES: Lazy<i32> = Lazy::new(|| {
    use std::env;
    use fluvio_protocol::Encoder;
    use fluvio_spu_schema::fetch::FetchResponse;
    use fluvio_spu_schema::fetch::FetchableTopicResponse;
    use fluvio_spu_schema::fetch::FetchablePartitionResponse;

    use fluvio_protocol::record::MemoryRecords;

    let var_value = env::var("FLV_CLIENT_MAX_FETCH_BYTES").unwrap_or_default();
    let max_bytes: i32 = var_value.parse().unwrap_or_else(|_| {
        FetchResponse::<MemoryRecords>::default().write_size(0) as i32
            + FetchableTopicResponse::<MemoryRecords>::default().write_size(0) as i32
            + FetchablePartitionResponse::<MemoryRecords>::default().write_size(0) as i32
            + FLUVIO_MAX_SIZE_TOPIC_NAME as i32 // using max size of topic name
            + FLUVIO_CLIENT_MAX_FETCH_BYTES
    });
    max_bytes
});

/// Configures the behavior of consumer fetching and streaming
#[derive(Debug, Builder, Clone)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct ConsumerConfig {
    #[builder(default)]
    disable_continuous: bool,
    #[builder(default = "*MAX_FETCH_BYTES")]
    pub(crate) max_bytes: i32,
    #[builder(default)]
    pub(crate) isolation: Isolation,
    #[builder(default)]
    pub(crate) smartmodule: Vec<SmartModuleInvocation>,
    #[builder(default)]
    pub(crate) derivedstream:
        Option<fluvio_spu_schema::server::stream_fetch::DerivedStreamInvocation>,
}

impl ConsumerConfig {
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::default()
    }
}

impl ConsumerConfigBuilder {
    pub fn build(&self) -> Result<ConsumerConfig, FluvioError> {
        let config = self.build_impl().map_err(|e| {
            FluvioError::ConsumerConfig(format!("Missing required config option: {e}"))
        })?;
        Ok(config)
    }
}

/// Strategy used to select which partitions and from which topics should be streamed by the [`MultiplePartitionConsumer`]
pub enum PartitionSelectionStrategy {
    /// Consume from all the partitions of a given topic
    All(String),
    /// Consume from a given list of topics and partitions
    Multiple(Vec<(String, PartitionId)>),
}

impl PartitionSelectionStrategy {
    async fn selection(
        &self,
        spu_pool: Arc<SpuPool>,
    ) -> Result<Vec<(String, PartitionId)>, FluvioError> {
        let pairs = match self {
            PartitionSelectionStrategy::All(topic) => {
                let topics = spu_pool.metadata.topics();
                let topic_spec = topics
                    .lookup_by_key(topic)
                    .await?
                    .ok_or_else(|| FluvioError::TopicNotFound(topic.to_string()))?
                    .spec;
                let partition_count = topic_spec.partitions();
                (0..(partition_count as PartitionId))
                    .map(|partition| (topic.clone(), partition))
                    .collect::<Vec<_>>()
            }
            PartitionSelectionStrategy::Multiple(topic_partition) => topic_partition.to_owned(),
        };
        Ok(pairs)
    }
}
pub struct MultiplePartitionConsumer {
    strategy: PartitionSelectionStrategy,
    pool: Arc<SpuPool>,
    metrics: Arc<ClientMetrics>,
}

impl MultiplePartitionConsumer {
    pub(crate) fn new(
        strategy: PartitionSelectionStrategy,
        pool: Arc<SpuPool>,
        metrics: Arc<ClientMetrics>,
    ) -> Self {
        Self {
            strategy,
            pool,
            metrics,
        }
    }

    /// Continuously streams events from a particular offset in the selected partitions
    ///
    /// Streaming is one of the two ways to consume events in Fluvio.
    /// It is a continuous request for new records arriving in the selected partitions,
    /// beginning at a particular offset. You specify the starting point of the
    /// stream using an [`Offset`] and periodically receive events, either individually
    /// or in batches.
    ///
    /// If you want more fine-grained control over how records are streamed,
    /// check out the [`stream_with_config`] method.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{MultiplePartitionConsumer, FluvioError};
    /// # use fluvio::{Offset, ConsumerConfig};
    /// # mod futures {
    /// #     pub use futures_util::stream::StreamExt;
    /// # }
    /// # async fn example(consumer: &MultiplePartitionConsumer) -> Result<(), FluvioError> {
    /// use futures::StreamExt;
    /// let mut stream = consumer.stream(Offset::beginning()).await?;
    /// while let Some(Ok(record)) = stream.next().await {
    ///     let key = record.key().map(|key| String::from_utf8_lossy(key).to_string());
    ///     let value = String::from_utf8_lossy(record.value()).to_string();
    ///     println!("Got event: key={:?}, value={}", key, value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Offset`]: struct.Offset.html
    /// [`ConsumerConfig`]: struct.ConsumerConfig.html
    /// [`stream_with_config`]: struct.ConsumerConfig.html#method.stream_with_config
    #[instrument(skip(self, offset))]
    pub async fn stream(
        &self,
        offset: Offset,
    ) -> Result<impl Stream<Item = Result<Record, ErrorCode>>, FluvioError> {
        let config = ConsumerConfig::builder().build()?;
        let stream = self.stream_with_config(offset, config).await?;

        Ok(stream)
    }

    /// Continuously streams events from a particular offset in the selected partitions
    ///
    /// Most of the time, you shouldn't need to use a custom [`ConsumerConfig`].
    /// If you don't know what these settings do, try checking out the simpler
    /// [`stream`] method that uses the default streaming settings.
    ///
    /// Streaming is one of the two ways to consume events in Fluvio.
    /// It is a continuous request for new records arriving in the selected partitions,
    /// beginning at a particular offset. You specify the starting point of the
    /// stream using an [`Offset`] and a [`ConsumerConfig`], and periodically
    /// receive events, either individually or in batches.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{MultiplePartitionConsumer, FluvioError};
    /// # use fluvio::{Offset, ConsumerConfig};
    /// # mod futures {
    /// #     pub use futures_util::stream::StreamExt;
    /// # }
    /// # async fn example(consumer: &MultiplePartitionConsumer) -> Result<(), FluvioError> {
    /// use futures::StreamExt;
    /// // Use a custom max_bytes value in the config
    /// let fetch_config = ConsumerConfig::builder()
    ///     .max_bytes(1000)
    ///     .build()?;
    /// let mut stream = consumer.stream_with_config(Offset::beginning(), fetch_config).await?;
    /// while let Some(Ok(record)) = stream.next().await {
    ///     let key: Option<String> = record.key().map(|key| String::from_utf8_lossy(key).to_string());
    ///     let value = String::from_utf8_lossy(record.value());
    ///     println!("Got record: key={:?}, value={}", key, value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Offset`]: struct.Offset.html
    /// [`ConsumerConfig`]: struct.ConsumerConfig.html
    #[instrument(skip(self, offset, config))]
    pub async fn stream_with_config(
        &self,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<impl Stream<Item = Result<Record, ErrorCode>>, FluvioError> {
        let consumers = self
            .strategy
            .selection(self.pool.clone())
            .await?
            .into_iter()
            .map(|(topic, partition)| {
                PartitionConsumer::new(
                    topic,
                    partition as PartitionId,
                    self.pool.clone(),
                    self.metrics.clone(),
                )
            })
            .collect::<Vec<_>>();

        let streams_future = consumers
            .iter()
            .map(|consumer| consumer.stream_with_config(offset.clone(), config.clone()));

        let streams_result = join_all(streams_future).await;

        let streams = streams_result.into_iter().collect::<Result<Vec<_>, _>>()?;

        Ok(select_all(streams))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_config_default() {
        let _config = ConsumerConfig::builder().build().unwrap();
    }
}
