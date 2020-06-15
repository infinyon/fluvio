use std::fmt::Debug;
use std::fmt::Display;
use std::fmt;
use std::io::Error as IoError;
use std::marker::PhantomData;

use bytes::BytesMut;
use log::trace;

use kf_protocol::Encoder;
use kf_protocol::Decoder;
use kf_protocol::Version;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use kf_protocol::api::RecordSet;
use kf_protocol::api::Request;
use kf_protocol::api::ErrorCode;
use kf_protocol::fs::KfFileRecordSet;
use kf_protocol::fs::StoreValue;
use kf_protocol::fs::FileWrite;
use flv_storage::SlicePartitionResponse;
use flv_future_aio::fs::AsyncFileSlice;

use super::KfFollowerPeerApiEnum;

pub type FileSyncRequest = SyncRequest<KfFileRecordSet>;
pub type DefaultSyncRequest = SyncRequest<RecordSet>;
pub type PeerFilePartitionResponse = PeerFetchablePartitionResponse<KfFileRecordSet>;
pub type PeerFileTopicResponse = PeerFetchableTopicResponse<KfFileRecordSet>;

/// used for sending records and commits
/// re purpose topic response since it has records and commit offsets
#[derive(Default, Encode, Decode, Debug)]
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
            write!(f, "{},", topic)?;
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
    const API_KEY: u16 = KfFollowerPeerApiEnum::SyncRecords as u16;
    const DEFAULT_API_VERSION: i16 = 7;
    type Response = SyncResponse;
}

#[derive(Default, Encode, Decode, Debug)]
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

#[derive(Encode, Decode, Default, Debug)]
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
            write!(f, "{},", partition)?;
        }
        write!(f, "]")
    }
}

#[derive(Encode, Decode, Default, Debug)]
pub struct PeerFetchablePartitionResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub partition_index: i32,
    pub error_code: ErrorCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub records: R,
}

impl<R> fmt::Display for PeerFetchablePartitionResponse<R>
where
    R: Encoder + Decoder + Default + Debug + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "p: {}, hw: {} {}",
            self.partition_index, self.high_watermark, self.records
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
        self.partition_index.encode(src, version)?;
        self.error_code.encode(src, version)?;
        self.high_watermark.encode(src, version)?;
        self.last_stable_offset.encode(src, version)?;
        self.records.file_encode(src, data, version)?;
        Ok(())
    }
}

impl SlicePartitionResponse for PeerFilePartitionResponse {
    fn set_hw(&mut self, offset: i64) {
        self.high_watermark = offset;
    }

    fn set_slice(&mut self, slice: AsyncFileSlice) {
        self.records = slice.into();
    }

    fn set_error_code(&mut self, error: ErrorCode) {
        self.error_code = error;
    }

    fn set_last_stable_offset(&mut self, offset: i64) {
        self.last_stable_offset = offset;
    }

    fn set_log_start_offset(&mut self, _offset: i64) {}
}
