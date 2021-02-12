use std::fmt::Debug;
use std::marker::PhantomData;
use std::io::Error as IoError;

use log::trace;
use bytes::BytesMut;

use crate::core::Decoder;
use crate::core::Encoder;
use crate::core::Version;
use crate::derive::Encode;
use crate::derive::Decode;
use crate::derive::FluvioDefault;

use crate::record::RecordSet;
use crate::record::FileRecordSet;
use crate::ErrorCode;
use crate::store::FileWrite;
use crate::store::StoreValue;

pub type DefaultFetchResponse = FetchResponse<RecordSet>;
pub type FileFetchResponse = FetchResponse<FileRecordSet>;
pub type FileTopicResponse = FetchableTopicResponse<FileRecordSet>;
pub type FilePartitionResponse = FetchablePartitionResponse<FileRecordSet>;

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct FetchResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    #[fluvio(min_version = 1, ignorable)]
    pub throttle_time_ms: i32,

    #[fluvio(min_version = 7)]
    pub error_code: ErrorCode,

    /// The fetch session ID, or 0 if this is not part of a fetch session.
    #[fluvio(min_version = 7)]
    pub session_id: i32,

    /// The response topics.
    pub topics: Vec<FetchableTopicResponse<R>>,
}

impl<R> FetchResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub fn find_partition(
        self,
        topic: &str,
        partition: i32,
    ) -> Option<FetchablePartitionResponse<R>> {
        for topic_res in self.topics {
            if topic_res.name == topic {
                for partition_res in topic_res.partitions {
                    if partition_res.partition_index == partition {
                        return Some(partition_res);
                    }
                }
            }
        }

        None
    }
}

impl FileWrite for FileFetchResponse {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding FileFetchResponse");
        trace!("encoding throttle_time_ms {}", self.throttle_time_ms);
        self.throttle_time_ms.encode(src, version)?;
        trace!("encoding error code {:#?}", self.error_code);
        self.error_code.encode(src, version)?;
        trace!("encoding session code {}", self.session_id);
        self.session_id.encode(src, version)?;
        trace!("encoding topics len: {}", self.topics.len());
        self.topics.file_encode(src, data, version)?;
        Ok(())
    }
}

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct FetchableTopicResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    /// The topic name.
    pub name: String,

    /// The topic partitions.
    pub partitions: Vec<FetchablePartitionResponse<R>>,
    pub data: PhantomData<R>,
}

impl FileWrite for FileTopicResponse {
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

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct FetchablePartitionResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    /// The partiiton index.
    pub partition_index: i32,

    /// The error code, or 0 if there was no fetch error
    pub error_code: ErrorCode,

    /// The current high water mark.
    pub high_watermark: i64,

    /// The last stable offset (or LSO) of the partition. This is the last offset such that the
    /// state of all transactional records prior to this offset have been decided (ABORTED or
    /// COMMITTED)
    #[fluvio(min_version = 4, ignorable)]
    pub last_stable_offset: i64,

    /// The current log start offset.
    #[fluvio(min_version = 5, ignorable)]
    pub log_start_offset: i64,

    /// The aborted transactions.
    #[fluvio(min_version = 4)]
    pub aborted: Option<Vec<AbortedTransaction>>,

    /// The record data.
    pub records: R,
}

impl FileWrite for FilePartitionResponse {
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
        self.log_start_offset.encode(src, version)?;
        self.aborted.encode(src, version)?;
        self.records.file_encode(src, data, version)?;
        Ok(())
    }
}

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct AbortedTransaction {
    /// The producer id associated with the aborted transaction.
    #[fluvio(min_version = 4)]
    pub producer_id: i64,

    /// The first offset in the aborted transaction.
    #[fluvio(min_version = 4)]
    pub first_offset: i64,
}

// -----------------------------------
// Implementation
// -----------------------------------

impl<R> FetchResponse<R>
where
    R: Encoder + Decoder + Debug,
{
    pub fn find_topic(&self, topic: &str) -> Option<&FetchableTopicResponse<R>>
    where
        R: Debug,
    {
        for r_topic in &self.topics {
            if r_topic.name == *topic {
                return Some(r_topic);
            }
        }
        None
    }

}