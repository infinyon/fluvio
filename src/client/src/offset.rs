use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::{debug, trace};
use dataplane::ReplicaKey;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetPartitionResponse;

use crate::FluvioError;
use crate::client::SerialFrame;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OffsetInner {
    Absolute(i64),
    FromBeginning(i64),
    FromEnd(i64),
}

/// Describes the location of an event stored in a Fluvio partition
///
/// All Fluvio events are stored as a log inside a partition. A log
/// is just an ordered list, and an `Offset` is just a way to select
/// an item from that list. There are several ways that an `Offset`
/// may identify an element from a log. Suppose you sent some
/// multiples of `11` to your partition. The various offset types
/// would look like this:
///
/// ```text
///         Partition Log: [ 00, 11, 22, 33, 44, 55, 66 ]
///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
///  FromBeginning Offset:    0,  1,  2,  3,  4,  5,  6
///        FromEnd Offset:    6,  5,  4,  3,  2,  1,  0
/// ```
///
/// When a new partition is created, it always starts counting new
/// events at the `Absolute` offset of 0. An absolute offset is a
/// unique index that represents the event's distance from the very
/// beginning of the partition. The absolute offset of an event never
/// changes.
///
/// Sometimes when a partition gets very large, Fluvio will begin
/// deleting events from the beginning of the log in order to save
/// space. Whenever it does this, it keeps track of the latest
/// non-deleted event. This allows the `FromBeginning` offset to
/// select an event which is a certain number of places in front of
/// the deleted range. For example, let's say that the first two
/// events from our partition were deleted. Our new offsets would
/// look like this:
///
/// ```text
///                          These events were deleted!
///                          |
///                          vvvvvv
///         Partition Log: [ .., .., 22, 33, 44, 55, 66 ]
///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
///  FromBeginning Offset:            0,  1,  2,  3,  4
///        FromEnd Offset:    6,  5,  4,  3,  2,  1,  0
/// ```
///
/// Just like the `FromBeginning` offset may change if events are deleted,
/// the `FromEnd` offset will change when new events are added. Let's take
/// a look:
///
/// ```text
///                                        These events were added!
///                                                               |
///                                                      vvvvvvvvvv
///         Partition Log: [ .., .., 22, 33, 44, 55, 66, 77, 88, 99 ]
///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6,  7,  8,  9
///  FromBeginning Offset:            0,  1,  2,  3,  4,  5,  6,  7
///        FromEnd Offset:    9,  8,  7,  6,  5,  4,  3,  2,  1,  0
/// ```
///
/// # Example
///
/// All offsets must be constructed with a positive index. Negative
/// numbers are meaningless for offsets and therefore are not allowed.
/// Trying to construct an offset with a negative number will yield `None`.
///
/// ```
/// use fluvio::Offset;
/// let absolute_offset = Offset::absolute(5).unwrap();
/// let offset_from_beginning = Offset::from_beginning(100).unwrap();
/// let offset_from_end = Offset::from_end(10).unwrap();
///
/// // Negative offsets will give None
/// assert_eq!(Offset::absolute(-10), None);
/// assert_eq!(Offset::from_beginning(-15), None);
/// assert_eq!(Offset::from_end(-20), None);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Offset {
    inner: OffsetInner,
}

impl Offset {
    /// Creates an absolute offset with the given index
    ///
    /// The index must not be less than zero.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::Offset;
    /// assert!(Offset::absolute(100).is_some());
    /// assert!(Offset::absolute(0).is_some());
    /// assert!(Offset::absolute(-10).is_none());
    /// ```
    pub fn absolute(index: i64) -> Option<Offset> {
        if index < 0 {
            return None;
        }
        Some(Self {
            inner: OffsetInner::Absolute(index),
        })
    }

    /// Creates a relative offset starting at the beginning of the saved log
    ///
    /// A relative `FromBeginning` offset will not always match an `Absolute`
    /// offset. In order to save space, Fluvio may sometimes delete events
    /// from the beginning of the log. When this happens, the `FromBeginning`
    /// relative offset starts counting from the first non-deleted log entry.
    ///
    /// ```text
    ///                          These events were deleted!
    ///                          |
    ///                          vvvvvv
    ///         Partition Log: [ .., .., 22, 33, 44, 55, 66 ]
    ///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
    ///  FromBeginning Offset:            0,  1,  2,  3,  4
    /// ```
    ///
    /// The offset must not be less than zero.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::Offset;
    /// assert!(Offset::from_beginning(10).is_some());
    /// assert!(Offset::from_beginning(0).is_some());
    /// assert!(Offset::from_beginning(-10).is_none());
    /// ```
    pub fn from_beginning(offset: i64) -> Option<Offset> {
        if offset < 0 {
            return None;
        }
        Some(Self {
            inner: OffsetInner::FromBeginning(offset),
        })
    }

    /// Creates a relative offset counting backwards from the end of the log
    ///
    /// A relative `FromEnd` offset will begin counting from the last
    /// "stable committed" event entry in the log. Increasing the offset will
    /// select events in reverse chronological order from the most recent event
    /// towards the earliest event. Therefore, a relative `FromEnd` offset may
    /// refer to different entries depending on when a query is made.
    ///
    /// For example, `Offset::from_end(3)` will refer to the event with content
    /// `33` at this point in time:
    ///
    /// ```text
    ///         Partition Log: [ .., .., 22, 33, 44, 55, 66 ]
    ///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
    ///        FromEnd Offset:    6,  5,  4,  3,  2,  1,  0
    /// ```
    ///
    /// But when these new events are added, `Offset::from_end(3)` will refer to
    /// the event with content `66`:
    ///
    /// ```text
    ///                                        These events were added!
    ///                                                               |
    ///                                                      vvvvvvvvvv
    ///         Partition Log: [ .., .., 22, 33, 44, 55, 66, 77, 88, 99 ]
    ///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6,  7,  8,  9
    ///        FromEnd Offset:    9,  8,  7,  6,  5,  4,  3,  2,  1,  0
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::Offset;
    /// assert!(Offset::from_end(10).is_some());
    /// assert!(Offset::from_end(0).is_some());
    /// assert!(Offset::from_end(-10).is_none());
    /// ```
    pub fn from_end(offset: i64) -> Option<Offset> {
        if offset < 0 {
            return None;
        }
        Some(Self {
            inner: OffsetInner::FromEnd(offset),
        })
    }

    /// Converts this offset into an absolute offset
    ///
    /// If this offset is relative from the beginning (i.e. it was created
    /// using the [`from_beginning`] function), then using `to_absolute` will
    /// calculate the absolute offset by finding the first still-persisted
    /// event offset and adding the relative offset to it.
    ///
    /// Similarly, if this offset is relative from the end (i.e. it was created
    /// using the [`from_end`] function), then using `to_absolute` will calculate
    /// the absolute offset by finding the last stably-committed event and subtracting
    /// the relative offset from it.
    ///
    /// Calling `to_absolute` on an offset that is already absolute just returns
    /// that same offset.
    ///
    /// Note that calculating relative offsets requires connecting to Fluvio, and
    /// therefore it is `async` and returns a `Result`.
    pub(crate) async fn to_absolute<F, S: Into<String>>(
        &self,
        client: &mut F,
        topic: S,
        partition: i32,
    ) -> Result<i64, FluvioError>
    where
        F: SerialFrame,
    {
        let offset = match self.inner {
            OffsetInner::Absolute(offset) => offset,
            OffsetInner::FromBeginning(offset) => {
                let replica = ReplicaKey::new(topic, partition);
                let offsets = fetch_offsets(client, &replica).await?;
                offsets.start_offset + offset
            }
            OffsetInner::FromEnd(offset) => {
                let replica = ReplicaKey::new(topic, partition);
                let offsets = fetch_offsets(client, &replica).await?;
                offsets.last_stable_offset - offset
            }
        };

        Ok(offset)
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
