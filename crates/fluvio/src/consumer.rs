use std::collections::BTreeMap;
use std::sync::Arc;

use dataplane::api::DefaultRequestMiddleWare;
use futures_util::stream::{Stream, select_all};
use tracing::{debug, error, trace, instrument};
use once_cell::sync::Lazy;
use futures_util::future::{Either, err, join_all};
use futures_util::stream::{StreamExt, once, iter};
use futures_util::FutureExt;

use fluvio_spu_schema::server::stream_fetch::{
    DefaultStreamFetchRequest, DefaultStreamFetchResponse, SmartStreamPayload, SmartStreamWasm,
    SmartStreamKind, WASM_MODULE_V2_API, GZIP_WASM_API,
};
use dataplane::Isolation;
use dataplane::ReplicaKey;
use dataplane::ErrorCode;
use dataplane::fetch::DefaultFetchRequest;
use dataplane::fetch::FetchPartition;
use dataplane::fetch::FetchableTopic;
use dataplane::fetch::FetchablePartitionResponse;
use dataplane::record::RecordSet;
use dataplane::record::Record as DefaultRecord;
use dataplane::batch::Batch;
use fluvio_types::event::offsets::OffsetPublisher;

use crate::FluvioError;
use crate::offset::{Offset, fetch_offsets};
use crate::spu::SpuPool;
use derive_builder::Builder;

/// An interface for consuming events from a particular partition
///
/// There are two ways to consume events: by "fetching" events
/// and by "streaming" events. Fetching involves specifying a
/// range of events that you want to consume via their [`Offset`].
/// A fetch is a sort of one-time batch operation: you'll receive
/// all of the events in your range all at once. When you consume
/// events via Streaming, you specify a starting [`Offset`] and
/// receive an object that will continuously yield new events as
/// they arrive.
///
/// # Creating a Consumer
///
/// You can create a `PartitionConsumer` via the [`partition_consumer`]
/// method on the [`Fluvio`] client, like so:
///
/// ```
/// # use fluvio::{Fluvio, Offset, ConsumerConfig, FluvioError};
/// # async fn example(fluvio: &Fluvio) -> Result<(), FluvioError> {
/// let consumer = fluvio.partition_consumer("my-topic", 0).await?;
/// let records = consumer.fetch(Offset::beginning()).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Offset`]: struct.Offset.html
/// [`partition_consumer`]: struct.Fluvio.html#method.partition_consumer
/// [`Fluvio`]: struct.Fluvio.html
pub struct PartitionConsumer {
    topic: String,
    partition: i32,
    pool: Arc<SpuPool>,
}

impl PartitionConsumer {
    pub(crate) fn new(topic: String, partition: i32, pool: Arc<SpuPool>) -> Self {
        Self {
            topic,
            partition,
            pool,
        }
    }

    /// Returns the name of the Topic that this consumer reads from
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the ID of the partition that this consumer reads from
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Fetches events from a particular offset in the consumer's partition
    ///
    /// A "fetch" is one of the two ways to consume events in Fluvio.
    /// It is a batch request for records from a particular offset in
    /// the partition. You specify the position of records to retrieve
    /// using an [`Offset`], and receive the events as a list of records.
    ///
    /// If you want more fine-grained control over how records are fetched,
    /// check out the [`fetch_with_config`] method.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{PartitionConsumer, Offset, ConsumerConfig, FluvioError};
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// let response = consumer.fetch(Offset::beginning()).await?;
    /// for batch in response.records.batches {
    ///     for record in batch.records() {
    ///         let string = String::from_utf8_lossy(record.value.as_ref());
    ///         println!("Got record: {}", string);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Offset`]: struct.Offset.html
    /// [`fetch_with_config`]: struct.PartitionConsumer.html#method.fetch_with_config
    #[deprecated(note = "Use 'stream' instead", since = "0.9.2")]
    #[instrument(skip(self, offset))]
    pub async fn fetch(
        &self,
        offset: Offset,
    ) -> Result<FetchablePartitionResponse<RecordSet>, FluvioError> {
        let config = ConsumerConfig::builder().build()?;
        #[allow(deprecated)]
        let records = self.fetch_with_config(offset, config).await?;
        Ok(records)
    }

    /// Fetches events from a consumer using a specific fetching configuration
    ///
    /// Most of the time, you shouldn't need to use a custom [`ConsumerConfig`].
    /// If you don't know what these settings do, try checking out the simpler
    /// [`fetch`] method that uses the default fetching settings.
    ///
    /// A "fetch" is one of the two ways to consume events in Fluvio.
    /// It is a batch request for records from a particular offset in
    /// the partition. You specify the range of records to retrieve
    /// using an [`Offset`] and a [`ConsumerConfig`], and receive
    /// the events as a list of records.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{PartitionConsumer, FluvioError, Offset, ConsumerConfig};
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// // Use custom fetching configurations
    /// let fetch_config = ConsumerConfig::builder()
    ///     .max_bytes(1000)
    ///     .build()?;
    ///
    /// let response = consumer.fetch_with_config(Offset::beginning(), fetch_config).await?;
    /// for batch in response.records.batches {
    ///     for record in batch.records() {
    ///         let string = String::from_utf8_lossy(record.value.as_ref());
    ///         println!("Got record: {}", string);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`ConsumerConfig`]: struct.ConsumerConfig.html
    /// [`fetch`]: struct.PartitionConsumer.html#method.fetch
    /// [`Offset`]: struct.Offset.html
    #[deprecated(note = "Use 'stream_with_config' instead", since = "0.9.2")]
    #[instrument(skip(self, offset, option))]
    pub async fn fetch_with_config(
        &self,
        offset: Offset,
        option: ConsumerConfig,
    ) -> Result<FetchablePartitionResponse<RecordSet>, FluvioError> {
        let replica = ReplicaKey::new(&self.topic, self.partition);
        debug!(
            "starting fetch log once: {:#?} from replica: {}",
            offset, &replica,
        );

        let mut leader = self.pool.create_serial_socket(&replica).await?;
        let offsets = fetch_offsets(&mut leader, &replica).await?;
        debug!("found spu leader {}", leader);
        let offset = offset.resolve(&offsets).await?;

        let partition = FetchPartition {
            partition_index: self.partition,
            fetch_offset: offset,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let topic_request = FetchableTopic {
            name: self.topic.to_owned(),
            fetch_partitions: vec![partition],
        };

        let fetch_request = DefaultFetchRequest {
            topics: vec![topic_request],
            isolation_level: option.isolation,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let response = leader.send_receive(fetch_request).await?;

        debug!("received fetch logs for {}", &replica);

        if let Some(partition_response) = response.find_partition(&self.topic, self.partition) {
            debug!(
                "found partition response with: {} batches: {} bytes",
                partition_response.records.batches.len(),
                bytes_count(&partition_response.records)
            );
            Ok(partition_response)
        } else {
            Err(FluvioError::PartitionNotFound(
                self.topic.clone(),
                self.partition,
            ))
        }
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
    ) -> Result<impl Stream<Item = Result<Record, FluvioError>>, FluvioError> {
        let config = ConsumerConfig::builder().build()?;
        let stream = self.stream_with_config(offset, config).await?;

        Ok(stream)
    }

    /// Continuously streams events from a particular offset in the consumer's partition
    ///
    /// Most of the time, you shouldn't need to use a custom [`ConsumerConfig`].
    /// If you don't know what these settings do, try checking out the simpler
    /// [`stream`] method that uses the default streaming settings.
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
    ) -> Result<impl Stream<Item = Result<Record, FluvioError>>, FluvioError> {
        let stream = self.stream_batches_with_config(offset, config).await?;
        let partition = self.partition;
        let flattened =
            stream.flat_map(move |result: Result<Batch, _>| match result {
                Err(e) => Either::Right(once(err(e))),
                Ok(batch) => {
                    let base_offset = batch.base_offset;
                    let records = batch.own_records().into_iter().enumerate().map(
                        move |(relative, record)| {
                            Ok(Record {
                                partition,
                                offset: base_offset + relative as i64,
                                record,
                            })
                        },
                    );
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
    ) -> Result<impl Stream<Item = Result<Batch, FluvioError>>, FluvioError> {
        let stream = self.request_stream(offset, config).await?;
        let flattened = stream.flat_map(|batch_result: Result<DefaultStreamFetchResponse, _>| {
            let response = match batch_result {
                Ok(response) => response,
                Err(e) => return Either::Right(once(err(e))),
            };

            // If we ever get an error_code AND batches of records, we want to first send
            // the records down the consumer stream, THEN an Err with the error inside.
            // This way the consumer always gets to read all records that were properly
            // processed before hitting an error, so that the error does not obscure those records.
            let batches = response.partition.records.batches.into_iter().map(Ok);
            let error = {
                let code = response.partition.error_code;
                match code {
                    ErrorCode::None => None,
                    ErrorCode::SmartStreamError(error) => {
                        Some(Err(FluvioError::SmartStream(error)))
                    }
                    _ => Some(Err(FluvioError::AdminApi(
                        fluvio_sc_schema::ApiError::Code(code, None),
                    ))),
                }
            };

            let items = batches.chain(error.into_iter());
            Either::Left(iter(items))
        });

        Ok(flattened)
    }

    /// Creates a stream of `DefaultStreamFetchResponse` for older consumers who rely
    /// on the internal structure of the fetch response. New clients should use the
    /// `stream` and `stream_with_config` methods.
    #[instrument(skip(self, config))]
    async fn request_stream(
        &self,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<impl Stream<Item = Result<DefaultStreamFetchResponse, FluvioError>>, FluvioError>
    {
        use fluvio_future::task::spawn;
        use futures_util::stream::empty;
        use fluvio_spu_schema::server::stream_fetch::WASM_MODULE_API;
        use fluvio_protocol::api::Request;

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
            ..Default::default()
        };

        // add wasm module if SPU supports it
        let stream_fetch_version = serial_socket
            .versions()
            .lookup_version(DefaultStreamFetchRequest::API_KEY)
            .unwrap_or((WASM_MODULE_API - 1) as i16);

        if let Some(mut module) = config.wasm_module {
            if stream_fetch_version < WASM_MODULE_API as i16 {
                return Err(FluvioError::Other("SPU does not support WASM".to_owned()));
            }

            if stream_fetch_version < WASM_MODULE_V2_API as i16 {
                // SmartStream V1
                debug!("Using WASM V1 API");
                let wasm = module.wasm.get_raw()?;
                stream_request.wasm_module = wasm.into_owned();
            } else {
                // SmartStream V2
                debug!("Using WASM V2 API");
                if stream_fetch_version < GZIP_WASM_API as i16 {
                    module.wasm.to_raw()?;
                } else {
                    debug!("Using compressed WASM API");
                    module.wasm.to_gzip()?;
                }
                stream_request.wasm_payload = Some(module);
            }
        }
        let mut stream = self
            .pool
            .create_stream_with_version(&replica, stream_request, DefaultRequestMiddleWare::default(), stream_fetch_version)
            .await?;

        let ft_stream = async move {
            if let Some(Ok(response)) = stream.next().await {
                let stream_id = response.stream_id;

                trace!("first stream response: {:#?}", response);
                debug!(
                    stream_id,
                    last_offset = ?response.partition.next_offset_for_fetch(),
                    "first stream response"
                );

                let publisher = OffsetPublisher::shared(0);
                let mut listener = publisher.change_listner();

                // update stream with received offsets
                spawn(async move {
                    use fluvio_spu_schema::server::update_offset::{UpdateOffsetsRequest, OffsetUpdate};

                    loop {
                        let fetch_last_value = listener.listen().await;
                        debug!(fetch_last_value, stream_id, "received end fetch");
                        if fetch_last_value < 0 {
                            debug!("fetch last is end, terminating");
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
                let update_stream = stream.map(move |item| {
                    item.map(|response| {
                        if let Some(last_offset) = response.partition.next_offset_for_fetch() {
                            debug!(last_offset, stream_id, "received last offset from spu");
                            response_publisher.update(last_offset);
                        }
                        response
                    })
                    .map_err(|e| e.into())
                });
                Either::Left(
                    iter(vec![Ok(response)])
                        .chain(publish_stream::EndPublishSt::new(update_stream, publisher)),
                )
            } else {
                Either::Right(empty())
            }
        };

        let stream = if config.disable_continuous {
            TakeRecords::new(ft_stream.flatten_stream().boxed(), record_count).boxed()
        } else {
            ft_stream.flatten_stream().boxed()
        };

        Ok(stream)
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
    S: Stream<Item = Result<DefaultStreamFetchResponse, FluvioError>> + std::marker::Unpin,
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
    S: Stream<Item = Result<DefaultStreamFetchResponse, FluvioError>> + std::marker::Unpin,
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
                    .map(|it| it.records().len())
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

/// compute total bytes in record set
fn bytes_count(records: &RecordSet) -> usize {
    records
        .batches
        .iter()
        .map(|batch| {
            batch
                .records()
                .iter()
                .map(|record| record.value.len())
                .sum::<usize>()
        })
        .sum()
}

/// MAX FETCH BYTES
static MAX_FETCH_BYTES: Lazy<i32> = Lazy::new(|| {
    use std::env;
    let var_value = env::var("FLV_CLIENT_MAX_FETCH_BYTES").unwrap_or_default();
    let max_bytes: i32 = var_value.parse().unwrap_or(1000000);
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
    #[builder(private, default, setter(into, strip_option))]
    pub(crate) wasm_module: Option<SmartStreamPayload>,
}

impl ConsumerConfig {
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::default()
    }
}

impl ConsumerConfigBuilder {
    pub fn build(&self) -> Result<ConsumerConfig, FluvioError> {
        let config = self.build_impl().map_err(|e| {
            FluvioError::ConsumerConfig(format!("Missing required config option: {}", e))
        })?;
        Ok(config)
    }

    /// Adds a SmartStream filter to this ConsumerConfig
    pub fn wasm_filter<T: Into<Vec<u8>>>(
        &mut self,
        filter: T,
        params: BTreeMap<String, String>,
    ) -> &mut Self {
        self.wasm_module(SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(filter.into()),
            kind: SmartStreamKind::Filter,
            params: params.into(),
        });
        self
    }

    /// Adds a SmartStream map to this ConsumerConfig
    pub fn wasm_map<T: Into<Vec<u8>>>(
        &mut self,
        map: T,
        params: BTreeMap<String, String>,
    ) -> &mut Self {
        self.wasm_module(SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(map.into()),
            kind: SmartStreamKind::Map,
            params: params.into(),
        });
        self
    }

    /// Adds a SmartStream filter_map to this ConsumerConfig
    pub fn wasm_filter_map<T: Into<Vec<u8>>>(
        &mut self,
        filter_map: T,
        params: BTreeMap<String, String>,
    ) -> &mut Self {
        self.wasm_module(SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(filter_map.into()),
            kind: SmartStreamKind::FilterMap,
            params: params.into(),
        });
        self
    }

    /// Adds a SmartStream array_map to this ConsumerConfig
    pub fn wasm_array_map<T: Into<Vec<u8>>>(
        &mut self,
        array_map: T,
        params: BTreeMap<String, String>,
    ) -> &mut Self {
        self.wasm_module(SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(array_map.into()),
            kind: SmartStreamKind::ArrayMap,
            params: params.into(),
        })
    }

    /// Set a WASM aggregator function and initial accumulator value
    pub fn wasm_aggregate<T: Into<Vec<u8>>, U: Into<Vec<u8>>>(
        &mut self,
        aggregate: T,
        accumulator: U,
        params: BTreeMap<String, String>,
    ) -> &mut Self {
        self.wasm_module(SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(aggregate.into()),
            kind: SmartStreamKind::Aggregate {
                accumulator: accumulator.into(),
            },
            params: params.into(),
        });
        self
    }
}

/// Strategy used to select which partitions and from which topics should be streamed by the [`MultiplePartitionConsumer`]
pub enum PartitionSelectionStrategy {
    /// Consume from all the partitions of a given topic
    All(String),
    /// Consume from a given list of topics and partitions
    Multiple(Vec<(String, i32)>),
}

impl PartitionSelectionStrategy {
    async fn selection(&self, spu_pool: Arc<SpuPool>) -> Result<Vec<(String, i32)>, FluvioError> {
        let pairs = match self {
            PartitionSelectionStrategy::All(topic) => {
                let topics = spu_pool.metadata.topics();
                let topic_spec = topics
                    .lookup_by_key(topic)
                    .await?
                    .ok_or_else(|| FluvioError::TopicNotFound(topic.to_string()))?
                    .spec;
                let partition_count = topic_spec.partitions();
                (0..partition_count)
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
}

impl MultiplePartitionConsumer {
    pub(crate) fn new(strategy: PartitionSelectionStrategy, pool: Arc<SpuPool>) -> Self {
        Self { strategy, pool }
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
    ) -> Result<impl Stream<Item = Result<Record, FluvioError>>, FluvioError> {
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
    ) -> Result<impl Stream<Item = Result<Record, FluvioError>>, FluvioError> {
        let consumers = self
            .strategy
            .selection(self.pool.clone())
            .await?
            .into_iter()
            .map(|(topic, partition)| PartitionConsumer::new(topic, partition, self.pool.clone()))
            .collect::<Vec<_>>();

        let streams_future = consumers
            .iter()
            .map(|consumer| consumer.stream_with_config(offset.clone(), config.clone()));

        let streams_result = join_all(streams_future).await;

        let streams = streams_result.into_iter().collect::<Result<Vec<_>, _>>()?;

        Ok(select_all(streams))
    }
}

/// The individual record for a given stream.
pub struct Record {
    /// The offset of this Record into its partition
    offset: i64,
    /// The partition where this Record is stored
    partition: i32,
    /// The Record contents
    record: DefaultRecord,
}

impl Record {
    /// The offset from the initial offset for a given stream.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// The partition where this Record is stored.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the contents of this Record's key, if it exists
    pub fn key(&self) -> Option<&[u8]> {
        self.record.key().map(|it| it.as_ref())
    }

    /// Returns the contents of this Record's value
    pub fn value(&self) -> &[u8] {
        self.record.value().as_ref()
    }

    /// Returns the inner representation of the Record
    pub fn into_inner(self) -> DefaultRecord {
        self.record
    }
}

impl AsRef<[u8]> for Record {
    fn as_ref(&self) -> &[u8] {
        self.value()
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
