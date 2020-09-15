//!
//! # Continuous Fetch
//!
//! Stream records to client
//!
use std::fmt::Debug;
use std::io::Error as IoError;
use std::marker::PhantomData;

use log::trace;
use bytes::BytesMut;

use dataplane_protocol::core::Version;
use dataplane_protocol::core::Encoder;
use dataplane_protocol::core::Decoder;
use dataplane_protocol::api::Request;
use dataplane_protocol::derive::Decode;
use dataplane_protocol::derive::Encode;
use dataplane_protocol::StoreValue;
use dataplane_protocol::record::FileRecordSet;
use dataplane_protocol::FileWrite;
use dataplane_protocol::fetch::FetchablePartitionResponse;
use dataplane_protocol::record::RecordSet;
use dataplane_protocol::Isolation;

pub type DefaultStreamFetchResponse = StreamFetchResponse<RecordSet>;
pub type FileStreamFetchRequest = StreamFetchRequest<FileRecordSet>;
pub type DefaultStreamFetchRequest = StreamFetchRequest<RecordSet>;

use super::SpuServerApiKey;

/// Fetch records continuously
/// After initial fetch, update to same replica will stream to client
#[derive(Decode, Encode, Default, Debug)]
pub struct StreamFetchRequest<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub topic: String,
    pub partition: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
    pub isolation: Isolation,
    pub data: PhantomData<R>,
}

impl<R> Request for StreamFetchRequest<R>
where
    R: Debug + Decoder + Encoder,
{
    const API_KEY: u16 = SpuServerApiKey::StreamFetch as u16;
    const DEFAULT_API_VERSION: i16 = 10;
    type Response = StreamFetchResponse<R>;
}

#[derive(Encode, Decode, Default, Debug)]
pub struct StreamFetchResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub topic: String,
    pub partition: FetchablePartitionResponse<R>,
}

impl FileWrite for StreamFetchResponse<FileRecordSet> {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding FlvContinuousFetchResponse");
        trace!("topic {}", self.topic);
        self.topic.encode(src, version)?;
        self.partition.file_encode(src, data, version)?;
        Ok(())
    }
}
