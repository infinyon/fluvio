#![allow(deprecated)]

use std::fmt::{Debug, self};
use std::io;
use std::io::Error as IoError;
use std::io::Read;

use bytes::BufMut;
use flate2::{
    Compression,
    bufread::{GzEncoder, GzDecoder},
};

use fluvio_protocol::{Encoder, Decoder, Version};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;

// The fluvio COMMON_VERSION in fluvio-spu-schema/src/lib.rs
// that introduced the smartmodule name to SmartModuleInvocations
pub const COMMON_VERSION_HAS_SM_NAME: Version = 25;

/// The request payload when using a Consumer SmartModule.
///
/// This includes the WASM module name as well as the invocation being used.
/// It also carries any data that is required for specific invocations of SmartModules.
#[derive(Debug, Default, Clone)]
pub struct SmartModuleInvocation {
    pub wasm: SmartModuleInvocationWasm,
    pub kind: SmartModuleKind,
    pub params: SmartModuleExtraParams,
    // only included in PROD_API_HAS_SM_NAME, or later
    // if decoding a version before this, None will be filled in
    pub name: Option<String>, // option for backward compatibility
}

impl Decoder for SmartModuleInvocation {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: bytes::Buf,
    {
        self.wasm.decode(src, version)?;
        self.kind.decode(src, version)?;
        self.params.decode(src, version)?;
        if version < COMMON_VERSION_HAS_SM_NAME {
            self.name = None;
        } else {
            self.name.decode(src, version)?;
        }
        Ok(())
    }
}

impl Encoder for SmartModuleInvocation {
    fn write_size(&self, version: Version) -> usize {
        let mut size = self.wasm.write_size(version);
        size += self.kind.write_size(version);
        size += self.params.write_size(version);
        if version >= COMMON_VERSION_HAS_SM_NAME {
            size += self.name.write_size(version);
        }
        size
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        self.wasm.encode(dest, version)?;
        self.kind.encode(dest, version)?;
        self.params.encode(dest, version)?;
        if version >= COMMON_VERSION_HAS_SM_NAME {
            self.name.encode(dest, version)?;
        }
        Ok(())
    }
}

#[derive(Clone, Encoder, Decoder)]
pub enum SmartModuleInvocationWasm {
    /// Name of SmartModule
    #[fluvio(tag = 0)]
    Predefined(String),
    /// Compressed WASM module payload using Gzip
    #[fluvio(tag = 1)]
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
    #[fluvio(tag = 0)]
    Filter,
    #[fluvio(tag = 1)]
    Map,
    #[fluvio(tag = 2)]
    #[fluvio(min_version = ARRAY_MAP_WASM_API)]
    ArrayMap,
    #[fluvio(tag = 3)]
    Aggregate { accumulator: Vec<u8> },
    #[fluvio(tag = 4)]
    #[fluvio(min_version = ARRAY_MAP_WASM_API)]
    FilterMap,
    #[fluvio(tag = 5)]
    #[fluvio(min_version = SMART_MODULE_API, max_version = CHAIN_SMARTMODULE_API)]
    Join(String),
    #[fluvio(tag = 6)]
    #[fluvio(min_version = SMART_MODULE_API, max_version = CHAIN_SMARTMODULE_API)]
    JoinStream {
        topic: String,
        derivedstream: String,
    },
    #[fluvio(tag = 7)]
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
    #[fluvio(tag = 0)]
    None,
    #[fluvio(tag = 1)]
    Aggregate { accumulator: Vec<u8> },
    #[fluvio(tag = 2)]
    Join(String),
    #[fluvio(tag = 3)]
    JoinStream {
        topic: String,
        derivedstream: String,
    },
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
