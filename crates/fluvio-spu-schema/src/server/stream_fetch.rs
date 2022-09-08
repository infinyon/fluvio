//!
//! # Continuous Fetch
//!
//! Stream records to client
//!
use std::fmt::{Debug};
use std::marker::PhantomData;

use educe::Educe;
use fluvio_protocol::record::RawRecords;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::Request;

use fluvio_protocol::record::RecordSet;

use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;

use crate::fetch::FetchablePartitionResponse;
use crate::isolation::Isolation;

pub type DefaultStreamFetchResponse = StreamFetchResponse<RecordSet<RawRecords>>;
pub type DefaultStreamFetchRequest = StreamFetchRequest<RecordSet<RawRecords>>;

use super::SpuServerApiKey;
use super::smartmodule::{LegacySmartModulePayload, SmartModuleInvocation};

// version for WASM_MODULE
pub const WASM_MODULE_API: i16 = 11;
pub const WASM_MODULE_V2_API: i16 = 12;

// version for aggregator SmartModule
pub const AGGREGATOR_API: i16 = 13;

// version for gzipped WASM payloads
pub const GZIP_WASM_API: i16 = 14;

// version for SmartModule array map
pub const ARRAY_MAP_WASM_API: i16 = 15;

// version for persistent SmartModule
pub const SMART_MODULE_API: i16 = 16;

pub const GENERIC_SMARTMODULE_API: i16 = 17;

/// Fetch records continuously
/// Output will be send back as stream
#[derive(Decoder, Encoder, Default, Educe)]
#[educe(Debug)]
pub struct StreamFetchRequest<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub topic: String,
    pub partition: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
    pub isolation: Isolation,
    /// no longer used, but keep to avoid breaking compatibility, this will not be honored
    // TODO: remove in 0.10
    #[educe(Debug(ignore))]
    #[fluvio(min_version = 11)]
    pub wasm_module: Vec<u8>,
    // TODO: remove in 0.10
    #[fluvio(min_version = 12)]
    pub wasm_payload: Option<LegacySmartModulePayload>,
    #[fluvio(min_version = 16)]
    pub smartmodule: Option<SmartModuleInvocation>,
    #[fluvio(min_version = 16)]
    pub derivedstream: Option<DerivedStreamInvocation>,
    pub data: PhantomData<R>,
}

impl<R> Request for StreamFetchRequest<R>
where
    R: Debug + Decoder + Encoder,
{
    const API_KEY: u16 = SpuServerApiKey::StreamFetch as u16;
    const DEFAULT_API_VERSION: i16 = SMART_MODULE_API;
    type Response = StreamFetchResponse<R>;
}

///
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct DerivedStreamInvocation {
    pub stream: String,
    pub params: SmartModuleExtraParams,
}

#[derive(Encoder, Decoder, Default, Debug)]
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

    use fluvio_protocol::Version;
    use fluvio_protocol::store::{StoreValue, FileWrite};

    use crate::file::FileRecordSet;

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

#[cfg(test)]
mod tests {

    use crate::server::smartmodule::{SmartModuleKind, SmartModuleWasmCompressed};

    use super::*;

    #[test]
    fn test_encode_stream_fetch_request() {
        let mut dest = Vec::new();
        let value = DefaultStreamFetchRequest {
            topic: "one".to_string(),
            partition: 3,
            wasm_payload: Some(LegacySmartModulePayload {
                kind: SmartModuleKind::Filter,
                wasm: SmartModuleWasmCompressed::Raw(vec![0xde, 0xad, 0xbe, 0xef]),
                ..Default::default()
            }),
            ..Default::default()
        };
        value.encode(&mut dest, 12).expect("should encode");
        let expected = vec![
            0x00, 0x03, 0x6f, 0x6e, 0x65, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00,
        ];
        assert_eq!(dest, expected);
    }

    #[test]
    fn test_decode_stream_fetch_request() {
        let bytes = vec![
            0x00, 0x03, 0x6f, 0x6e, 0x65, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x0,
        ];
        let mut value = DefaultStreamFetchRequest::default();
        value.decode(&mut std::io::Cursor::new(bytes), 12).unwrap();
        assert_eq!(value.topic, "one");
        assert_eq!(value.partition, 3);
        let sm = match value.wasm_payload {
            Some(wasm) => wasm,
            _ => panic!("should have smartstreeam payload"),
        };
        let wasm = match sm.wasm {
            SmartModuleWasmCompressed::Raw(wasm) => wasm,
            #[allow(unreachable_patterns)]
            _ => panic!("should be SmartModuleWasm::Raw"),
        };
        assert_eq!(wasm, vec![0xde, 0xad, 0xbe, 0xef]);
        assert!(matches!(sm.kind, SmartModuleKind::Filter));
    }

    #[test]
    fn test_zip_unzip_works() {
        const ORIG_LEN: usize = 1024;
        let orig = SmartModuleWasmCompressed::Raw(vec![0x01; ORIG_LEN]);
        let mut compressed = orig.clone();
        compressed.to_gzip().unwrap();
        assert!(
            matches!(&compressed, &SmartModuleWasmCompressed::Gzip(ref x) if x.len() < ORIG_LEN)
        );
        let mut uncompressed = compressed.clone();
        uncompressed.to_raw().unwrap();
        assert!(
            matches!((&uncompressed, &orig), (&SmartModuleWasmCompressed::Raw(ref x), &SmartModuleWasmCompressed::Raw(ref y)) if x == y )
        );
        assert_eq!(orig.get_raw().unwrap(), compressed.get_raw().unwrap());
    }
}
