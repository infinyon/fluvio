//!
//! # SmartModule Spec
//!

use fluvio_protocol::{Encoder, Decoder};

use super::SmartModuleMetadata;

#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleSpec {
    pub meta: Option<SmartModuleMetadata>,
    pub wasm: SmartModuleWasm,
}

impl SmartModuleSpec {
    pub fn pkg_name(&self) -> &str {
        self.meta
            .as_ref()
            .map(|meta| &meta.package.name as &str)
            .unwrap_or_else(|| "")
    }

    pub fn pkg_group(&self) -> &str {
        self.meta
            .as_ref()
            .map(|meta| &meta.package.group as &str)
            .unwrap_or_else(|| "")
    }

    pub fn pkg_version(&self) -> String {
        self.meta
            .as_ref()
            .map(|meta| meta.package.version.to_string())
            .unwrap_or_else(|| "".to_owned())
    }
}

#[derive(Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleWasm {
    pub format: SmartModuleWasmFormat,
    #[cfg_attr(feature = "use_serde", serde(with = "base64"))]
    pub payload: Vec<u8>,
}
impl std::fmt::Debug for SmartModuleWasm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "SmartModuleWasm {{ format: {:?}, payload: [REDACTED] }}",
            self.format
        ))
    }
}

impl SmartModuleWasm {
    /// Create SmartModule from compressed Gzip format
    pub fn from_compressed_gzip(payload: Vec<u8>) -> Self {
        SmartModuleWasm {
            payload,
            format: SmartModuleWasmFormat::Binary,
        }
    }

    #[cfg(feature = "smartmodule")]
    /// Create SmartModule from uncompressed Wasm format
    pub fn from_raw_wasm_bytes(raw_payload: &[u8]) -> std::io::Result<Self> {
        use std::io::Read;
        use flate2::{Compression, bufread::GzEncoder};

        let mut encoder = GzEncoder::new(raw_payload, Compression::default());
        let mut buffer = Vec::with_capacity(raw_payload.len());
        encoder.read_to_end(&mut buffer)?;

        Ok(Self::from_compressed_gzip(buffer))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleWasmFormat {
    #[cfg_attr(feature = "use_serde", serde(rename = "BINARY"))]
    Binary,
    #[cfg_attr(feature = "use_serde", serde(rename = "TEXT"))]
    Text,
}

impl Default for SmartModuleWasmFormat {
    fn default() -> SmartModuleWasmFormat {
        SmartModuleWasmFormat::Binary
    }
}

#[cfg(feature = "use_serde")]
mod base64 {
    use serde::{Serialize, Deserialize};
    use serde::{Deserializer, Serializer};

    #[allow(clippy::ptr_arg)]
    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = base64::encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::decode(base64.as_bytes()).map_err(serde::de::Error::custom)
    }
}
