//!
//! # Continuous Fetch
//!
//! Stream records to client
//!
use std::fmt::Debug;
use std::marker::PhantomData;

use dataplane::core::Encoder;
use dataplane::core::Decoder;
use dataplane::api::Request;
use dataplane::derive::Decode;
use dataplane::derive::Encode;
use dataplane::fetch::FetchablePartitionResponse;
use dataplane::record::RecordSet;
use dataplane::Isolation;

pub type DefaultStreamFetchResponse = StreamFetchResponse<RecordSet>;

pub type DefaultStreamFetchRequest = StreamFetchRequest<RecordSet>;

use super::SpuServerApiKey;

// version for WASM_MODULE
pub const WASM_MODULE_API: i16 = 11;

// version for aggregator smartstream
pub const AGGREGATOR_API: i16 = 12;

/// Fetch records continuously
/// Output will be send back as stream
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
    #[fluvio(min_version = 11)]
    pub wasm_module: Vec<u8>,
    /// The initial value of the accumulator during aggregation
    #[fluvio(min_version = 12)]
    pub accumulator: Option<Vec<u8>>,
    pub data: PhantomData<R>,
}

impl<R> Request for StreamFetchRequest<R>
where
    R: Debug + Decoder + Encoder,
{
    const API_KEY: u16 = SpuServerApiKey::StreamFetch as u16;
    const DEFAULT_API_VERSION: i16 = AGGREGATOR_API;
    type Response = StreamFetchResponse<R>;
}

#[derive(Encode, Decode, Default, Debug)]
pub struct StreamFetchResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub topic: String,
    pub stream_id: u32,
    pub partition: FetchablePartitionResponse<R>,
}

#[cfg(feature = "file")]
pub use file::*;

#[cfg(feature = "file")]
mod file {

    use std::io::Error as IoError;

    use log::trace;
    use bytes::BytesMut;

    use dataplane::core::Version;
    use dataplane::store::StoreValue;
    use dataplane::record::FileRecordSet;
    use dataplane::store::FileWrite;

    pub type FileStreamFetchRequest = StreamFetchRequest<FileRecordSet>;

    use super::*;

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
            self.stream_id.encode(src, version)?;
            self.partition.file_encode(src, data, version)?;
            Ok(())
        }
    }
}
