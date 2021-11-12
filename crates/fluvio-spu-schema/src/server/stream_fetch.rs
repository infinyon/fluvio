//!
//! # Continuous Fetch
//!
//! Stream records to client
//!
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::io::{self, Read};
use std::borrow::Cow;

use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;
use dataplane::fetch::FetchablePartitionResponse;
use dataplane::record::RecordSet;
use dataplane::Isolation;
use dataplane::smartmodule::SmartModuleExtraParams;

use flate2::{
    Compression,
    bufread::{GzEncoder, GzDecoder},
};

pub type DefaultStreamFetchResponse = StreamFetchResponse<RecordSet>;

pub type DefaultStreamFetchRequest = StreamFetchRequest<RecordSet>;

use super::SpuServerApiKey;

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

/// Fetch records continuously
/// Output will be send back as stream
#[derive(Decoder, Encoder, Default, Debug)]
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
    #[fluvio(min_version = 11)]
    pub wasm_module: Vec<u8>,
    // TODO: remove in 0.10
    #[fluvio(min_version = 12)]
    pub wasm_payload: Option<LegacySmartModulePayload>,
    #[fluvio(min_version = 16)]
    pub smartmodule: Option<SmartModuleInvocation>,
    #[fluvio(min_version = 16)]
    pub smartstream: Option<SmartStreamInvocation>,
    #[fluvio(min_version = 16)]
    pub table_format: Option<SmartStreamInvocation>,
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

/// The request payload when using a Consumer SmartModule.
///
/// This includes the WASM content as well as the type of SmartModule being used.
/// It also carries any data that is required for specific types of SmartModules.
/// TODO: remove in 0.10
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct LegacySmartModulePayload {
    pub wasm: SmartModuleWasmCompressed,
    pub kind: SmartModuleKind,
    pub params: SmartModuleExtraParams,
}

/// The request payload when using a Consumer SmartModule.
///
/// This includes the WASM module name as well as the invocation being used.
/// It also carries any data that is required for specific invocations of SmartModules.
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartModuleInvocation {
    pub wasm: SmartModuleInvocationWasm,
    pub kind: SmartModuleKind,
    pub params: SmartModuleExtraParams,
}

#[derive(Debug, Clone, Encoder, Decoder)]
pub enum SmartModuleInvocationWasm {
    /// Name of SmartModule
    Predefined(String),
    /// Compressed WASM module payload using Gzip
    AdHoc(Vec<u8>),
}

impl Default for SmartModuleInvocationWasm {
    fn default() -> Self {
        Self::AdHoc(Vec::new())
    }
}

/// Indicates the type of SmartModule as well as any special data required
#[derive(Debug, Clone, Encoder, Decoder)]
pub enum SmartModuleKind {
    Filter,
    Map,
    Aggregate {
        accumulator: Vec<u8>,
    },
    #[fluvio(min_version = ARRAY_MAP_WASM_API)]
    ArrayMap,
    #[fluvio(min_version = ARRAY_MAP_WASM_API)]
    FilterMap,
    #[fluvio(min_version = SMART_MODULE_API)]
    Join(String),
    #[fluvio(min_version = SMART_MODULE_API)]
    JoinStream {
        topic: String,
        smartstream: String,
    },
}

impl Default for SmartModuleKind {
    fn default() -> Self {
        Self::Filter
    }
}

/// Different possible representations of WASM modules.
///
/// In a fetch request, a WASM module may be given directly in the request
/// as raw bytes.
///
// TODO: remove in 0.10
#[derive(Clone, Encoder, Decoder)]
pub enum SmartModuleWasmCompressed {
    Raw(Vec<u8>),
    /// compressed WASM module payload using Gzip
    #[fluvio(min_version = 14)]
    Gzip(Vec<u8>),
    // TODO implement named WASM modules once we have a WASM store
    // Url(String),
}

fn zip(raw: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(raw, Compression::default());
    let mut buffer = Vec::with_capacity(raw.len());
    encoder.read_to_end(&mut buffer)?;
    Ok(buffer)
}

fn unzip(compressed: &[u8]) -> io::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(compressed);
    let mut buffer = Vec::with_capacity(compressed.len());
    decoder.read_to_end(&mut buffer)?;
    Ok(buffer)
}

impl SmartModuleWasmCompressed {
    /// returns the gzip-compressed WASM module bytes
    pub fn to_gzip(&mut self) -> io::Result<()> {
        if let Self::Raw(raw) = self {
            *self = Self::Gzip(zip(raw.as_ref())?);
        }
        Ok(())
    }

    /// returns the raw WASM module bytes
    pub fn to_raw(&mut self) -> io::Result<()> {
        if let Self::Gzip(gzipped) = self {
            *self = Self::Raw(unzip(gzipped)?);
        }
        Ok(())
    }

    /// get the raw bytes of the WASM module
    pub fn get_raw(&self) -> io::Result<Cow<[u8]>> {
        Ok(match self {
            Self::Raw(raw) => Cow::Borrowed(raw),
            Self::Gzip(gzipped) => Cow::Owned(unzip(gzipped.as_ref())?),
        })
    }
}

impl Default for SmartModuleWasmCompressed {
    fn default() -> Self {
        Self::Raw(Vec::new())
    }
}

impl Debug for SmartModuleWasmCompressed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raw(bytes) => f
                .debug_tuple("Raw")
                .field(&format!("{} bytes", bytes.len()))
                .finish(),
            Self::Gzip(bytes) => f
                .debug_tuple("Gzip")
                .field(&format!("{} bytes", bytes.len()))
                .finish(),
        }
    }
}

///
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartStreamInvocation {
    pub stream: String,
    pub params: SmartModuleExtraParams,
}


pub enum TableInputKind {
    Json
}

pub enum TableOutputKind {
    Json
}

/// The request payload when a Consumer is using Table output with formatting details
/// 
/// - Specify selection keys for standard input formats ()
/// - Standard transformations to apply to values
#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct TableFormatInvocation {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_smartmodulekind() {
        let mut dest = Vec::new();
        let value: SmartModuleKind = SmartModuleKind::Filter;
        value.encode(&mut dest, 0).expect("should encode");
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 0x00);
    }

    #[test]
    fn test_decode_smartmodulekind() {
        let bytes = vec![0x01];
        let mut value: SmartModuleKind = Default::default();
        value
            .decode(&mut std::io::Cursor::new(bytes), 0)
            .expect("should decode");
        assert!(matches!(value, SmartModuleKind::Map));
    }

    #[test]
    fn test_encode_smartmodulewasm() {
        let mut dest = Vec::new();
        let value: SmartModuleWasmCompressed =
            SmartModuleWasmCompressed::Raw(vec![0xde, 0xad, 0xbe, 0xef]);
        value.encode(&mut dest, 0).expect("should encode");
        println!("{:02x?}", &dest);
        assert_eq!(dest.len(), 9);
        assert_eq!(dest[0], 0x00);
        assert_eq!(dest[1], 0x00);
        assert_eq!(dest[2], 0x00);
        assert_eq!(dest[3], 0x00);
        assert_eq!(dest[4], 0x04);
        assert_eq!(dest[5], 0xde);
        assert_eq!(dest[6], 0xad);
        assert_eq!(dest[7], 0xbe);
        assert_eq!(dest[8], 0xef);
    }

    #[test]
    fn test_decode_smartmodulewasm() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef];
        let mut value: SmartModuleWasmCompressed = Default::default();
        value
            .decode(&mut std::io::Cursor::new(bytes), 0)
            .expect("should decode");
        let inner = match value {
            SmartModuleWasmCompressed::Raw(inner) => inner,
            #[allow(unreachable_patterns)]
            _ => panic!("should decode to SmartModuleWasm::Raw"),
        };
        assert_eq!(inner.len(), 4);
        assert_eq!(inner[0], 0xde);
        assert_eq!(inner[1], 0xad);
        assert_eq!(inner[2], 0xbe);
        assert_eq!(inner[3], 0xef);
    }

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
