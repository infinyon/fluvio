use std::io::Read;
use std::{io, borrow::Cow};
use std::fmt::{Debug, self};

use flate2::{
    Compression,
    bufread::{GzEncoder, GzDecoder},
};

use fluvio_protocol::{Encoder, Decoder};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;

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

#[derive(Clone, Encoder, Decoder)]
pub enum SmartModuleInvocationWasm {
    /// Name of SmartModule
    Predefined(String),
    /// Compressed WASM module payload using Gzip
    AdHoc(Vec<u8>),
}

impl SmartModuleInvocationWasm {
    pub fn adhoc_from_bytes(bytes: &[u8]) -> io::Result<Self> {
        Ok(Self::AdHoc(zip(bytes)?))
    }
}

impl Default for SmartModuleInvocationWasm {
    fn default() -> Self {
        Self::AdHoc(Vec::new())
    }
}

impl Debug for SmartModuleInvocationWasm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Predefined(module) => write!(f, "Predefined{}", module),
            Self::AdHoc(bytes) => f
                .debug_tuple("Adhoc")
                .field(&format!("{} bytes", bytes.len()))
                .finish(),
        }
    }
}

/// Indicates the type of SmartModule as well as any special data required
#[derive(Debug, Clone, Encoder, Decoder, Default)]
pub enum SmartModuleKind {
    #[default]
    Filter,
    Map,
    #[fluvio(min_version = ARRAY_MAP_WASM_API)]
    ArrayMap,
    Aggregate {
        accumulator: Vec<u8>,
    },
    #[fluvio(min_version = ARRAY_MAP_WASM_API)]
    FilterMap,
    #[fluvio(min_version = SMART_MODULE_API)]
    Join(String),
    #[fluvio(min_version = SMART_MODULE_API)]
    JoinStream {
        topic: String,
        derivedstream: String,
    },
    #[fluvio(min_version = GENERIC_SMARTMODULE_API)]
    Generic(SmartModuleContextData),
}

impl std::fmt::Display for SmartModuleKind {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let name = match self {
            SmartModuleKind::Filter => "filter",
            SmartModuleKind::Map => "map",
            SmartModuleKind::ArrayMap => "array_map",
            SmartModuleKind::Aggregate { .. } => "aggregate",
            SmartModuleKind::FilterMap => "filter_map",
            SmartModuleKind::Join(..) => "join",
            SmartModuleKind::JoinStream { .. } => "join_stream",
            SmartModuleKind::Generic(..) => "smartmodule",
        };
        out.write_str(name)
    }
}

#[derive(Debug, Clone, Encoder, Decoder, Default)]
pub enum SmartModuleContextData {
    #[default]
    None,
    Aggregate {
        accumulator: Vec<u8>,
    },
    Join(String),
    JoinStream {
        topic: String,
        derivedstream: String,
    },
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
    fn test_gzip_smartmoduleinvocationwasm() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        let value: SmartModuleInvocationWasm =
            SmartModuleInvocationWasm::adhoc_from_bytes(&bytes).expect("should encode");
        if let SmartModuleInvocationWasm::AdHoc(compressed_bytes) = value {
            let decompressed_bytes = unzip(&compressed_bytes).expect("should decompress");
            assert_eq!(decompressed_bytes, bytes);
        } else {
            panic!("not adhoc")
        }
    }
}
