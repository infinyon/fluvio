use std::sync::Arc;
use std::convert::TryFrom;
use std::string::FromUtf8Error;

use futures_util::stream::Stream;
use tracing::{debug, error};
use once_cell::sync::Lazy;
use futures_util::future::{Either, err};
use futures_util::stream::{StreamExt, once, iter};

use fluvio_spu_schema::server::stream_fetch::{DefaultStreamFetchRequest, DefaultStreamFetchResponse};
use dataplane::Isolation;
use dataplane::ReplicaKey;
use dataplane::fetch::DefaultFetchRequest;
use dataplane::fetch::FetchPartition;
use dataplane::fetch::FetchableTopic;
use dataplane::fetch::FetchablePartitionResponse;
use dataplane::record::RecordSet;
use dataplane::record::DefaultRecord;
use fluvio_types::event::offsets::OffsetPublisher;

use crate::FluvioError;
use crate::offset::Offset;
use crate::client::SerialFrame;
use crate::spu::SpuPool;

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
/// ```no_run
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
    /// ```no_run
    /// # use fluvio::{PartitionConsumer, Offset, ConsumerConfig, FluvioError};
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// let response = consumer.fetch(Offset::beginning()).await?;
    /// for batch in response.records.batches {
    ///     for record in batch.records {
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
    pub async fn fetch(
        &self,
        offset: Offset,
    ) -> Result<FetchablePartitionResponse<RecordSet>, FluvioError> {
        let records = self
            .fetch_with_config(offset, ConsumerConfig::default())
            .await?;
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
    /// ```no_run
    /// # use fluvio::{PartitionConsumer, FluvioError, Offset, ConsumerConfig};
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// // Use custom fetching configurations
    /// let fetch_config = ConsumerConfig::default()
    ///     .with_max_bytes(1000);
    ///
    /// let response = consumer.fetch_with_config(Offset::beginning(), fetch_config).await?;
    /// for batch in response.records.batches {
    ///     for record in batch.records {
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

        debug!("found spu leader {}", leader);

        let offset = offset
            .to_absolute(&mut leader, &self.topic, self.partition)
            .await?;

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
    /// ```no_run
    /// # use fluvio::{PartitionConsumer, FluvioError};
    /// # use fluvio::{Offset, ConsumerConfig};
    /// # mod futures {
    /// #     pub use futures_util::stream::StreamExt;
    /// # }
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// use futures::StreamExt;
    /// let mut stream = consumer.stream(Offset::beginning()).await?;
    /// while let Some(Ok(record)) = stream.next().await {
    ///     let string = String::from_utf8_lossy(record.as_ref());
    ///     println!("Got event: {}", string);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Offset`]: struct.Offset.html
    /// [`ConsumerConfig`]: struct.ConsumerConfig.html
    /// [`stream_with_config`]: struct.ConsumerConfig.html#method.stream_with_config
    pub async fn stream(
        &self,
        offset: Offset,
    ) -> Result<impl Stream<Item = Result<Record, FluvioError>>, FluvioError> {
        let stream = self
            .stream_with_config(offset, ConsumerConfig::default())
            .await?;

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
    /// ```no_run
    /// # use fluvio::{PartitionConsumer, FluvioError};
    /// # use fluvio::{Offset, ConsumerConfig};
    /// # mod futures {
    /// #     pub use futures_util::stream::StreamExt;
    /// # }
    /// # async fn example(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// use futures::StreamExt;
    /// // Use a custom max_bytes value in the config
    /// let fetch_config = ConsumerConfig::default()
    ///     .with_max_bytes(1000);
    /// let mut stream = consumer.stream_with_config(Offset::beginning(), fetch_config).await?;
    /// while let Some(Ok(record)) = stream.next().await {
    ///     let string = String::from_utf8_lossy(record.as_ref());
    ///     println!("Got event: {}", string);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`Offset`]: struct.Offset.html
    /// [`ConsumerConfig`]: struct.ConsumerConfig.html
    pub async fn stream_with_config(
        &self,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<impl Stream<Item = Result<Record, FluvioError>>, FluvioError> {
        let stream = self._stream_batches_with_config(offset, config).await?;
        let flattened = stream.flat_map(|batch_result| {
            let batch = match batch_result {
                Ok(batch) => batch,
                Err(e) => return Either::Right(once(err(e))),
            };

            let records = batch
                .partition
                .records
                .batches
                .into_iter()
                .flat_map(|batch| {
                    let base_offset = batch.base_offset;
                    batch
                        .records
                        .into_iter()
                        .enumerate()
                        .map(move |(relative, record)| {
                            Ok(Record {
                                offset: base_offset + relative as i64,
                                record,
                            })
                        })
                });
            Either::Left(iter(records))
        });

        Ok(flattened)
    }

    /// Creates a stream of `DefaultStreamFetchResponse` for older consumers who rely
    /// on the internal structure of the fetch response. New clients should use the
    /// `stream` and `stream_with_config` methods.
    #[doc(hidden)]
    pub async fn _stream_batches_with_config(
        &self,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<impl Stream<Item = Result<DefaultStreamFetchResponse, FluvioError>>, FluvioError>
    {
        use fluvio_future::task::spawn;
        use futures_util::stream::empty;

        let replica = ReplicaKey::new(&self.topic, self.partition);
        debug!(
            "starting fetch log once: {:#?} from replica: {}",
            offset, &replica,
        );

        let mut serial_socket = self.pool.create_serial_socket(&replica).await?;
        debug!("created serial socket {}", serial_socket);
        let offset = offset
            .to_absolute(&mut serial_socket, &self.topic, self.partition)
            .await?;

        let stream_request = DefaultStreamFetchRequest {
            topic: self.topic.to_owned(),
            partition: self.partition,
            fetch_offset: offset,
            isolation: config.isolation,
            ..Default::default()
        };

        let mut stream = self.pool.create_stream(&replica, stream_request).await?;

        if let Some(first_response) = stream.next().await {
            if let Ok(response) = first_response {
                let stream_id = response.stream_id;

                debug!(stream_id,"got stream id from stream");

                let publisher = OffsetPublisher::shared(0);
                let mut listener = publisher.change_listner();

                // update stream with received offsets
                spawn(async move {
                    use fluvio_spu_schema::server::update_offset::{UpdateOffsetsRequest, OffsetUpdate};

                    loop {
                        let fetch_last_value = listener.listen().await;
                        if fetch_last_value < 0 {
                            debug!(fetch_last_value, stream_id, "received end fetch");
                            break;
                        } else {
                            let response = serial_socket
                                .send_receive(UpdateOffsetsRequest {
                                    offsets: vec![OffsetUpdate {
                                        offset: fetch_last_value,
                                        session_id: stream_id,
                                    }],
                                })
                                .await;
                            if let Err(err) = response {
                                error!("error sending offset: {:#?}", err);
                            }
                        }
                    }
                });

                let response_publisher = publisher.clone();
                let update_stream = stream.map(move |item| {
                    item.map(|response| {
                        if let Some(last_offset) = response.partition.records.last_offset() {
                            debug!(last_offset, stream_id);
                            response_publisher.update(last_offset);
                        }
                        response
                    })
                    .map_err(|e| e.into())
                });
                Ok(Either::Left(iter(vec![Ok(response)]).chain(
                    publish_stream::EndPublishSt::new(update_stream, publisher),
                )))
            } else {
                Ok(Either::Right(empty()))
            }
        } else {
            Ok(Either::Right(empty()))
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
                .records
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
#[derive(Debug)]
pub struct ConsumerConfig {
    pub(crate) max_bytes: i32,
    pub(crate) isolation: Isolation,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            max_bytes: *MAX_FETCH_BYTES,
            isolation: Isolation::default(),
        }
    }
}

impl ConsumerConfig {
    /// Maximum number of bytes to be fetched at a time.
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }
}

/// The individual record for a given stream.
pub struct Record {
    offset: i64,
    record: DefaultRecord,
}

impl Record {
    /// The offset from the initial offset for a given stream.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn to_string_lossy(&self) -> String {
        String::from_utf8_lossy(self.as_ref()).to_string()
    }
}

impl AsRef<[u8]> for Record {
    fn as_ref(&self) -> &[u8] {
        self.record.value.as_ref()
    }
}

impl TryFrom<Record> for String {
    type Error = FromUtf8Error;
    fn try_from(record: Record) -> Result<String, FromUtf8Error> {
        String::from_utf8(record.as_ref().to_vec())
    }
}

mod offset_update {}
