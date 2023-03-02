use std::fmt::Debug;
use std::marker::PhantomData;

use fluvio_protocol::record::BatchRecords;
use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::derive::FluvioDefault;
use fluvio_protocol::record::RecordSet;
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::record::Offset;

pub type DefaultFetchResponse = FetchResponse<RecordSet>;

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct FetchResponse<R> {
    /// The duration in milliseconds for which the request was throttled due to a quota violation,
    /// or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    pub error_code: ErrorCode,
    pub session_id: i32,

    /// The response topics.
    pub topics: Vec<FetchableTopicResponse<R>>,
}

impl<R> FetchResponse<R> {
    pub fn find_partition(
        self,
        topic: &str,
        partition: u32,
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

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct FetchableTopicResponse<R> {
    /// The topic name.
    pub name: String,

    /// The topic partitions.
    pub partitions: Vec<FetchablePartitionResponse<R>>,
    pub data: PhantomData<R>,
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct FetchablePartitionResponse<R> {
    /// The partition index.
    pub partition_index: PartitionId,

    /// The error code, or 0 if there was no fetch error
    pub error_code: ErrorCode,

    /// The current high water mark.
    pub high_watermark: i64,

    /// next offset to fetch in case of filter
    /// consumer should return that back to SPU, otherwise SPU will re-turn same filter records
    #[fluvio(min_version = 11, ignorable)]
    pub next_filter_offset: i64,

    /// The current log start offset.
    pub log_start_offset: i64,

    /// The aborted transactions.
    pub aborted: Option<Vec<AbortedTransaction>>,

    /// The record data.
    pub records: R,
}

impl<R: BatchRecords> FetchablePartitionResponse<RecordSet<R>> {
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

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

// -----------------------------------
// Implementation
// -----------------------------------

impl<R> FetchResponse<R> {
    pub fn find_topic(&self, topic: &str) -> Option<&FetchableTopicResponse<R>>
    where
        R: Debug,
    {
        self.topics.iter().find(|&r_topic| r_topic.name == *topic)
    }
}

#[cfg(feature = "file")]
pub use file::*;
use fluvio_types::PartitionId;

#[cfg(feature = "file")]
mod file {

    use std::io::Error as IoError;

    use tracing::trace;
    use bytes::BytesMut;

    use fluvio_protocol::store::FileWrite;
    use fluvio_protocol::store::StoreValue;
    use fluvio_protocol::core::Version;

    use crate::file::FileRecordSet;

    use super::*;

    pub type FileFetchResponse = FetchResponse<FileRecordSet>;
    pub type FileTopicResponse = FetchableTopicResponse<FileRecordSet>;
    pub type FilePartitionResponse = FetchablePartitionResponse<FileRecordSet>;

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
