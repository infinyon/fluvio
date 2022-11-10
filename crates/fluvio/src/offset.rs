use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::{debug, trace};
use fluvio_protocol::record::ReplicaKey;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetPartitionResponse;

use crate::FluvioError;
use fluvio_socket::VersionedSerialSocket;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OffsetInner {
    Absolute(i64),
    FromBeginning(i64),
    FromEnd(i64),
}

impl OffsetInner {
    fn resolve(&self, offsets: &FetchOffsetPartitionResponse) -> i64 {
        match self {
            Self::Absolute(offset) => *offset,
            Self::FromBeginning(offset) => {
                let resolved = offsets.start_offset + offset;
                resolved.clamp(offsets.start_offset, offsets.last_stable_offset)
            }
            Self::FromEnd(offset) => {
                let resolved = offsets.last_stable_offset - offset;
                resolved.clamp(offsets.start_offset, offsets.last_stable_offset)
            }
        }
    }
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
/// Fluvio allows you to set a retention policy that determines how
/// long your events should live. Once an event has outlived your
/// retention policy, Fluvio may delete it to save resources. Whenever
/// it does this, it keeps track of the latest non-deleted event. This
/// allows the `FromBeginning` offset to select an event which is a
/// certain number of places in front of the deleted range. For example,
/// let's say that the first two events from our partition were deleted.
/// Our new offsets would look like this:
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
/// let offset_from_beginning = Offset::from_beginning(100);
/// let offset_from_end = Offset::from_end(10);
///
/// // Negative values are not allowed for absolute offsets
/// assert!(Offset::absolute(-10).is_err());
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
    /// assert!(Offset::absolute(100).is_ok());
    /// assert!(Offset::absolute(0).is_ok());
    /// assert!(Offset::absolute(-10).is_err());
    /// ```
    pub fn absolute(index: i64) -> Result<Offset, FluvioError> {
        if index < 0 {
            return Err(FluvioError::NegativeOffset(index));
        }
        Ok(Self {
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
    ///                                   ^
    ///                                   |
    ///                 Offset::beginning()
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::Offset;
    /// let offset: Offset = Offset::beginning();
    /// ```
    pub fn beginning() -> Offset {
        Self::from_beginning(0)
    }

    /// Creates a relative offset a fixed distance after the oldest log entry
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
    ///                                                   ^
    ///                                                   |
    ///                           Offset::from_beginning(4)
    /// ```
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::Offset;
    /// // Creates an offset pointing 4 places after the oldest log entry
    /// let offset: Offset = Offset::from_beginning(4);
    /// ```
    pub fn from_beginning(offset: u32) -> Offset {
        Self {
            inner: OffsetInner::FromBeginning(offset as i64),
        }
    }

    /// Creates a relative offset pointing to the newest log entry
    ///
    /// A relative `FromEnd` offset will point to the last "stable committed"
    /// event entry in the log. Since a log may continue growing at any time,
    /// a `FromEnd` offset may refer to different entries depending on when a
    /// query is made.
    ///
    /// For example, `Offset::end()` will refer to the event with content
    /// `66` at this point in time:
    ///
    /// ```text
    ///         Partition Log: [ .., .., 22, 33, 44, 55, 66 ]
    ///       Absolute Offset:    0,  1,  2,  3,  4,  5,  6
    ///        FromEnd Offset:    6,  5,  4,  3,  2,  1,  0
    /// ```
    ///
    /// But when these new events are added, `Offset::end()` will refer to the
    /// event with content `99`.
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
    /// // Creates an offset pointing to the latest log entry
    /// let offset: Offset = Offset::end();
    /// ```
    pub fn end() -> Offset {
        Offset::from_end(0)
    }

    /// Creates a relative offset a fixed distance before the newest log entry
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
    /// // Creates an offset pointing 3 places before the latest log entry
    /// let offset: Offset = Offset::from_end(3);
    /// ```
    pub fn from_end(offset: u32) -> Offset {
        Self {
            inner: OffsetInner::FromEnd(offset as i64),
        }
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
    pub(crate) async fn resolve(
        &self,
        offsets: &FetchOffsetPartitionResponse,
    ) -> Result<i64, FluvioError> {
        let offset = self.inner.resolve(offsets);

        // Offset should never be less than 0, even for absolute
        let offset = offset.max(0);
        Ok(offset)
    }
}

pub(crate) async fn fetch_offsets(
    client: &mut VersionedSerialSocket,
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

    match response.find_partition(replica) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_beginning() {
        let offsets = FetchOffsetPartitionResponse {
            error_code: Default::default(),
            partition_index: 0,
            start_offset: 0,
            last_stable_offset: 10,
        };

        let offset_inner = OffsetInner::FromBeginning(3);
        let absolute = offset_inner.resolve(&offsets);
        assert_eq!(absolute, 3);
    }

    #[test]
    fn test_offset_beginning_start_nonzero() {
        let offsets = FetchOffsetPartitionResponse {
            error_code: Default::default(),
            partition_index: 0,
            start_offset: 5,
            last_stable_offset: 10,
        };

        let offset_inner = OffsetInner::FromBeginning(3);
        let absolute = offset_inner.resolve(&offsets);
        assert_eq!(absolute, 8);
    }

    #[test]
    fn test_offset_beginning_end_short() {
        let offsets = FetchOffsetPartitionResponse {
            error_code: Default::default(),
            partition_index: 0,
            start_offset: 0,
            last_stable_offset: 10,
        };

        let offset_inner = OffsetInner::FromBeginning(15);
        let absolute = offset_inner.resolve(&offsets);
        assert_eq!(absolute, 10);
    }

    #[test]
    fn test_offset_beginning_end_short_nonzero() {
        let offsets = FetchOffsetPartitionResponse {
            error_code: Default::default(),
            partition_index: 0,
            start_offset: 5,
            last_stable_offset: 10,
        };

        let offset_inner = OffsetInner::FromBeginning(15);
        let absolute = offset_inner.resolve(&offsets);
        assert_eq!(absolute, 10);
    }

    #[test]
    fn test_offset_end() {
        let offsets = FetchOffsetPartitionResponse {
            error_code: Default::default(),
            partition_index: 0,
            start_offset: 0,
            last_stable_offset: 10,
        };

        let offset_inner = OffsetInner::FromEnd(3);
        let absolute = offset_inner.resolve(&offsets);
        assert_eq!(absolute, 7);
    }

    #[test]
    fn test_offset_end_start_nonzero() {
        let offsets = FetchOffsetPartitionResponse {
            error_code: Default::default(),
            partition_index: 0,
            start_offset: 6,
            last_stable_offset: 10,
        };

        let offset_inner = OffsetInner::FromEnd(6);
        let absolute = offset_inner.resolve(&offsets);
        assert_eq!(absolute, 6);
    }

    #[test]
    fn test_offset_end_short() {
        let offsets = FetchOffsetPartitionResponse {
            error_code: Default::default(),
            partition_index: 0,
            start_offset: 0,
            last_stable_offset: 10,
        };

        let offset_inner = OffsetInner::FromEnd(100);
        let absolute = offset_inner.resolve(&offsets);
        assert_eq!(absolute, 0);
    }
}
