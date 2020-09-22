use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::debug;
use tracing::trace;

use kf_socket::AsyncResponse;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetPartitionResponse;
use fluvio_spu_schema::server::stream_fetch::DefaultStreamFetchRequest;
use dataplane::fetch::DefaultFetchRequest;
use dataplane::fetch::FetchPartition;
use dataplane::fetch::FetchableTopic;
use dataplane::fetch::FetchablePartitionResponse;
use dataplane::ReplicaKey;
use dataplane::record::RecordSet;
use dataplane::PartitionOffset;
use crate::FluvioError;
use crate::params::FetchOffset;
use crate::params::FetchLogOption;
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
/// # use fluvio::{Fluvio, FluvioError};
/// # use fluvio::params::{FetchOffset, FetchLogOption};
/// # async fn do_create_consumer(fluvio: &Fluvio) -> Result<(), FluvioError> {
/// let consumer = fluvio.partition_consumer("my-topic", 0).await?;
/// let records = consumer.fetch(FetchOffset::Earliest(None), FetchLogOption::default()).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Offset`]: ./struct.Offset.html
/// [`partition_consumer`]: ./struct.Fluvio#method.partition_consumer
/// [`Fluvio`]: ./struct.Fluvio
pub struct PartitionConsumer {
    topic: String,
    partition: i32,
    pool: SpuPool,
}

impl PartitionConsumer {
    pub(crate) fn new(topic: String, partition: i32, pool: SpuPool) -> Self {
        Self {
            topic,
            partition,
            pool,
        }
    }

    /// Fetches events from a particular offset in the consumer's partition
    ///
    /// A `Fetch` is one of the two ways to consume events in Fluvio.
    /// It is a batch request for records from a particular offset in
    /// the partition. You specify the range of records to retrieve
    /// using a [`FetchOffset`] and a [`FetchLogOption`], and receive
    /// the events as a list of records.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{PartitionConsumer, FluvioError};
    /// # use fluvio::params::{FetchOffset, FetchLogOption};
    /// # async fn do_fetch(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// // Fetch records starting from the earliest ones saved
    /// let offset = FetchOffset::Earliest(None);
    /// // Use default fetching configurations
    /// let fetch_config = FetchLogOption::default();
    ///
    /// let response = consumer.fetch(offset, fetch_config).await?;
    /// for batch in response.records.batches {
    ///     for record in batch.records {
    ///         if let Some(record) = record.value.inner_value() {
    ///             let string = String::from_utf8(record)
    ///                 .expect("record should be a string");
    ///             println!("Got record: {}", string);
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`FetchOffset`]: params/enum.FetchOffset.html
    /// [`FetchLogOption`]: params/struct.FetchLogOption.html
    pub async fn fetch(
        &self,
        offset: FetchOffset,
        option: FetchLogOption,
    ) -> Result<FetchablePartitionResponse<RecordSet>, FluvioError> {
        let replica = ReplicaKey::new(&self.topic, self.partition);
        debug!(
            "starting fetch log once: {:#?} from replica: {}",
            offset, &replica,
        );

        let mut leader = self.pool.create_serial_socket(&replica).await?;

        debug!("found spu leader {}", leader);

        let offset = calc_offset(&mut leader, &replica, offset).await?;

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
    /// stream using a [`FetchOffset`] and a [`FetchLogOption`], and periodically
    /// receive events, either individually or in batches.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio::{PartitionConsumer, FluvioError};
    /// # use fluvio::params::{FetchOffset, FetchLogOption};
    /// # async fn do_stream(consumer: &PartitionConsumer) -> Result<(), FluvioError> {
    /// // Start streaming events from the beginning of the partition
    /// let offset = FetchOffset::Earliest(None);
    /// // Use the default streaming settings
    /// let fetch_config = FetchLogOption::default();
    /// let mut stream = consumer.stream(offset, fetch_config).await?;
    /// while let Ok(event) = stream.next().await {
    ///     for batch in event.partition.records.batches {
    ///         for record in batch.records {
    ///             if let Some(record) = record.value.inner_value() {
    ///                 let string = String::from_utf8(record)
    ///                     .expect("record should be a string");
    ///                 println!("Got event: {}", string);
    ///             }
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`FetchOffset`]: params/enum.FetchOffset.html
    /// [`FetchLogOption`]: params/struct.FetchLogOption.html
    pub async fn stream(
        &self,
        offset_option: FetchOffset,
        option: FetchLogOption,
    ) -> Result<AsyncResponse<DefaultStreamFetchRequest>, FluvioError> {
        let replica = ReplicaKey::new(&self.topic, self.partition);
        debug!(
            "starting fetch log once: {:#?} from replica: {}",
            offset_option, &replica,
        );

        let mut serial_socket = self.pool.create_serial_socket(&replica).await?;
        debug!("created serial socket {}", serial_socket);
        let offset = calc_offset(&mut serial_socket, &replica, offset_option).await?;
        drop(serial_socket);

        let stream_request = DefaultStreamFetchRequest {
            topic: self.topic.to_owned(),
            partition: self.partition,
            fetch_offset: offset,
            isolation: option.isolation,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        self.pool.create_stream(&replica, stream_request).await
    }
}

async fn fetch_offsets<F: SerialFrame>(
    client: &mut F,
    replica: &ReplicaKey,
) -> Result<FetchOffsetPartitionResponse, FluvioError> {
    debug!("fetching offset for replica: {}", replica);

    let response = client
        .send_receive(FetchOffsetsRequest::new(
            replica.topic.to_owned(),
            replica.partition,
        ))
        .await?;

    trace!(
        "receive fetch response replica: {}, {:#?}",
        replica,
        response
    );

    match response.find_partition(&replica) {
        Some(partition_response) => {
            debug!("replica: {}, fetch offset: {}", replica, partition_response);
            Ok(partition_response)
        }
        None => Err(IoError::new(
            ErrorKind::InvalidData,
            format!("no replica offset for: {}", replica),
        )
        .into()),
    }
}

/// depends on offset option, calculate offset

async fn calc_offset<F: SerialFrame>(
    client: &mut F,
    replica: &ReplicaKey,
    offset: FetchOffset,
) -> Result<i64, FluvioError> {
    Ok(match offset {
        FetchOffset::Offset(inner_offset) => inner_offset,
        FetchOffset::Earliest(relative_offset) => {
            let offsets = fetch_offsets(client, replica).await?;
            offsets.start_offset() + relative_offset.unwrap_or(0)
        }
        FetchOffset::Latest(relative_offset) => {
            let offsets = fetch_offsets(client, replica).await?;
            offsets.last_stable_offset() - relative_offset.unwrap_or(0)
        }
    })
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
