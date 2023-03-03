#![allow(deprecated)]

use std::io::Read;
use std::io;
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
#[derive(Debug, Default, Clone, Encoder, Decoder)]
#[deprecated(
    since = "0.10.0",
    note = "will be removed in the next version. Use SmartModuleInvocation instead "
)]
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

    /// consume and get the raw bytes of the WASM module
    pub fn into_raw(self) -> io::Result<Vec<u8>> {
        match self {
            Self::AdHoc(gzipped) => Ok(unzip(gzipped.as_ref())?),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unable to represent as raw data",
            )),
        }
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
            Self::Predefined(module) => write!(f, "Predefined{module}"),
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
#[deprecated(
    since = "0.10.0",
    note = "will be removed in the next version. Use SmartModuleInvocationWasm instead"
)]
#[derive(Clone, Encoder, Decoder, Debug)]
pub enum SmartModuleWasmCompressed {
    Raw(Vec<u8>),
    /// compressed WASM module payload using Gzip
    #[fluvio(min_version = 14)]
    Gzip(Vec<u8>),
    // TODO implement named WASM modules once we have a WASM store
    // Url(String),
}

impl Default for SmartModuleWasmCompressed {
    fn default() -> Self {
        Self::Raw(Default::default())
    }
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
            .decode(&mut io::Cursor::new(bytes), 0)
            .expect("should decode");
        assert!(matches!(value, SmartModuleKind::Map));
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
