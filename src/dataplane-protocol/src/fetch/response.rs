use std::fmt::Debug;
use std::marker::PhantomData;

use crate::core::Decoder;
use crate::core::Encoder;
use crate::derive::Encode;
use crate::derive::Decode;
use crate::derive::FluvioDefault;

use crate::record::RecordSet;
use crate::ErrorCode;
use crate::Offset;

pub type DefaultFetchResponse = FetchResponse<RecordSet>;

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct FetchResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    pub error_code: ErrorCode,
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

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct FetchablePartitionResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    /// The partiiton index.
    pub partition_index: i32,

    /// The error code, or 0 if there was no fetch error
    pub error_code: ErrorCode,

    #[fluvio(min_version = 12)]
    pub smartstream_error: Option<SmartStreamError>,

    /// The current high water mark.
    pub high_watermark: i64,

    /// The last stable offset (or LSO) of the partition which is inherited from Kafka semamntics
    #[deprecated(since = "0.4.0", note = "Please use high_watermark")]
    pub last_stable_offset: i64,

    /// next offset to fetch in case of filter
    /// consumer should return that back to SPU, othewise SPU will re-turn same filter records
    #[fluvio(min_version = 11, ignorable)]
    pub next_filter_offset: i64,

    /// The current log start offset.
    pub log_start_offset: i64,

    /// The aborted transactions.
    pub aborted: Option<Vec<AbortedTransaction>>,

    /// The record data.
    pub records: R,
}

impl FetchablePartitionResponse<RecordSet> {
    /// offset that will be use for fetching rest of offsets
    /// this will be 1 greater than last offset of previous query
    /// If all records have been read then it will be either HW or LEO
    pub fn next_offset_for_fetch(&self) -> Option<Offset> {
        if self.next_filter_offset > 0 {
            Some(self.next_filter_offset)
        } else {
            self.records.last_offset()
        }
    }
}

#[derive(Encode, Decode, FluvioDefault, Debug)]
pub struct AbortedTransaction {
    pub producer_id: i64,
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

#[cfg(feature = "file")]
pub use file::*;

#[cfg(feature = "file")]
mod file {

    use std::io::Error as IoError;

    use tracing::trace;
    use bytes::BytesMut;

    use crate::record::FileRecordSet;
    use crate::store::FileWrite;
    use crate::store::StoreValue;
    use crate::core::Version;

    use super::*;

    pub type FileFetchResponse = FetchResponse<FileRecordSet>;
    pub type FileTopicResponse = FetchableTopicResponse<FileRecordSet>;
    pub type FilePartitionResponse = FetchablePartitionResponse<FileRecordSet>;

    impl FileWrite for FilePartitionResponse {
        #[allow(deprecated)]
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
            if version >= 11 {
                self.next_filter_offset.encode(src, version)?;
            } else {
                tracing::trace!("v: {} is less than last fetched version 11", version);
            }
            self.log_start_offset.encode(src, version)?;
            self.aborted.encode(src, version)?;
            self.records.file_encode(src, data, version)?;
            Ok(())
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
}
