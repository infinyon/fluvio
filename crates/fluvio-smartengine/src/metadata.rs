use std::collections::BTreeMap;
use std::io::{self, Read};
use std::borrow::Cow;

use flate2::{
    Compression,
    bufread::{GzEncoder, GzDecoder},
};

use dataplane::core::{Encoder, Decoder};

#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct LegacySmartModulePayload {
    pub wasm: SmartModuleWasmCompressed,
    pub kind: SmartModuleKind,
    pub params: SmartModuleExtraParams,
}

#[derive(Debug, Clone, Encoder, Decoder)]
pub enum SmartModuleWasmCompressed {
    Raw(Vec<u8>),
    /// compressed WASM module payload using Gzip
    //#[fluvio(min_version = 14)]
    Gzip(Vec<u8>),
    // TODO implement named WASM modules once we have a WASM store
    // Url(String),
}

impl Default for SmartModuleWasmCompressed {
    fn default() -> Self {
        Self::Raw(Vec::new())
    }
}

impl SmartModuleWasmCompressed {

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

    
    /// returns the gzip-compressed WASM module bytes
    pub fn to_gzip(&mut self) -> io::Result<()> {
        if let Self::Raw(raw) = self {
            *self = Self::Gzip(Self::zip(raw.as_ref())?);
        }
        Ok(())
    }

    /// returns the raw WASM module bytes
    pub fn to_raw(&mut self) -> io::Result<()> {
        if let Self::Gzip(gzipped) = self {
            *self = Self::Raw(Self::unzip(gzipped)?);
        }
        Ok(())
    }

    /// get the raw bytes of the WASM module
    pub fn get_raw(&self) -> io::Result<Cow<[u8]>> {
        Ok(match self {
            Self::Raw(raw) => Cow::Borrowed(raw),
            Self::Gzip(gzipped) => Cow::Owned(Self::unzip(gzipped.as_ref())?),
        })
    }
}


#[derive(Debug, Clone, Default, Encoder, Decoder)]
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

#[derive(Debug, Default, Clone, Encoder, Decoder)]
pub struct SmartModuleExtraParams {
    inner: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Encoder, Decoder)]
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
