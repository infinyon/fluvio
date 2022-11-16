use std::fmt::Debug;
use std::marker::PhantomData;

use fluvio_protocol::api::Request;
use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::derive::FluvioDefault;
use fluvio_protocol::record::RecordSet;
use fluvio_types::PartitionId;

use crate::isolation::Isolation;

use super::FetchResponse;

pub type DefaultFetchRequest = FetchRequest<RecordSet>;

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct FetchRequest<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait: i32,

    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,

    /// The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
    #[fluvio(min_version = 3, ignorable)]
    pub max_bytes: i32,

    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED
    /// (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1),
    /// non-transactional and COMMITTED transactional records are visible. To be more concrete,
    /// READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable
    /// offset), and enables the inclusion of the list of aborted transactions in the result, which
    /// allows consumers to discard ABORTED transactional records
    #[fluvio(min_version = 4)]
    pub isolation_level: Isolation,

    /// The topics to fetch.
    pub topics: Vec<FetchableTopic>,

    /// In an incremental fetch request, the partitions to remove.
    #[fluvio(min_version = 7)]
    pub forgotten: Vec<ForgottenTopic>,

    pub data: PhantomData<R>,
}

impl<R> Request for FetchRequest<R>
where
    R: Debug + Decoder + Encoder,
{
    const API_KEY: u16 = 1;

    const MIN_API_VERSION: i16 = 0;
    const MAX_API_VERSION: i16 = 10;
    const DEFAULT_API_VERSION: i16 = 10;

    type Response = FetchResponse<R>;
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct FetchableTopic {
    /// The name of the topic to fetch.
    pub name: String,

    /// The partitions to fetch.
    pub fetch_partitions: Vec<FetchPartition>,
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct ForgottenTopic {
    /// The partition name.
    #[fluvio(min_version = 7)]
    pub name: String,

    /// The partitions indexes to forget.
    #[fluvio(min_version = 7)]
    pub forgotten_partition_indexes: Vec<i32>,
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct FetchPartition {
    /// The partition index.
    pub partition_index: PartitionId,

    /// The current leader epoch of the partition.
    #[fluvio(min_version = 9, ignorable)]
    pub current_leader_epoch: i32,

    /// The message offset.
    pub fetch_offset: i64,

    /// The earliest available offset of the follower replica.  The field is only used when the
    /// request is sent by the follower.
    #[fluvio(min_version = 5)]
    pub log_start_offset: i64,

    /// The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may
    /// not be honored.
    pub max_bytes: i32,
}

#[cfg(feature = "file")]
pub use file::*;
#[cfg(feature = "file")]
mod file {
    use super::*;
    use crate::file::FileRecordSet;
    pub type FileFetchRequest = FetchRequest<FileRecordSet>;
}
