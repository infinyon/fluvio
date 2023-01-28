#![allow(clippy::assign_op_pattern)]

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt;
use std::io::Error as IoError;
use std::marker::PhantomData;

use bytes::BytesMut;
use fluvio_types::PartitionId;
use tracing::trace;

use fluvio_protocol::store::StoreValue;
use fluvio_protocol::store::FileWrite;
use fluvio_protocol::{Encoder, Decoder, Version};
use fluvio_protocol::record::{RecordSet};
use fluvio_protocol::api::Request;
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::record::RawRecords;
use fluvio_spu_schema::file::FileRecordSet;

use super::api_key::FollowerPeerApiEnum;

pub type FileSyncRequest = SyncRequest<FileRecordSet>;
pub type DefaultSyncRequest = SyncRequest<RecordSet<RawRecords>>;
pub type PeerFilePartitionResponse = PeerFetchablePartitionResponse<FileRecordSet>;
pub type PeerFileTopicResponse = PeerFetchableTopicResponse<FileRecordSet>;

/// used for sending records and commits
/// re purpose topic response since it has records and commit offsets
#[derive(Default, Encoder, Decoder, Debug)]
pub struct SyncRequest<R>
where
    R: Encoder + Decoder + Debug,
{
    pub topics: Vec<PeerFetchableTopicResponse<R>>,
}

impl<R> fmt::Display for SyncRequest<R>
where
    R: Encoder + Decoder + Debug + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        for topic in &self.topics {
            write!(f, "{topic},")?;
        }
        write!(f, "]")
    }
}

// Request trait
// Note that DEFAULT_API_VERSION is 7 which is required in order to map all fields for file encoding
// TODO: come up with unify encoding
impl<R> Request for SyncRequest<R>
where
    R: Encoder + Decoder + Debug,
{
    const API_KEY: u16 = FollowerPeerApiEnum::SyncRecords as u16;
    const DEFAULT_API_VERSION: i16 = 7;
    type Response = SyncResponse;
}

#[derive(Default, Encoder, Decoder, Debug)]
pub struct SyncResponse {}

/// allow sync request to be encoded for zerocopy
impl FileWrite for FileSyncRequest {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding for FileSyncRequest version: {}", version);
        self.topics.file_encode(src, data, version)?;
        Ok(())
    }
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct PeerFetchableTopicResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub name: String,
    pub partitions: Vec<PeerFetchablePartitionResponse<R>>,
    pub data: PhantomData<R>,
}

impl<R> fmt::Display for PeerFetchableTopicResponse<R>
where
    R: Encoder + Decoder + Default + Debug + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} [", self.name)?;
        for partition in &self.partitions {
            write!(f, "{partition},")?;
        }
        write!(f, "]")
    }
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct PeerFetchablePartitionResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub partition: PartitionId,
    pub error: ErrorCode,
    pub hw: i64,
    pub leo: i64,
    pub records: R,
}

impl<R> fmt::Display for PeerFetchablePartitionResponse<R>
where
    R: Encoder + Decoder + Default + Debug + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "partition: {}, hw: {} {}",
            self.partition, self.hw, self.records
        )
    }
}

impl FileWrite for PeerFileTopicResponse {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding fetch topic response");
        self.name.encode(src, version)?;
        self.partitions.file_encode(src, data, version)?;
        Ok(())
    }
}

impl FileWrite for PeerFilePartitionResponse {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding fetch partition response");
        self.partition.encode(src, version)?;
        self.error.encode(src, version)?;
        self.hw.encode(src, version)?;
        self.leo.encode(src, version)?;
        self.records.file_encode(src, data, version)?;
        Ok(())
    }
}
