//!
//! # SmartModule Spec
//!
use std::{io::Error as IoError, borrow::Cow};

use bytes::BufMut;
use tracing::debug;

#[cfg(feature = "smartmodule_validation")]
use wasmparser::{Parser, Chunk, Payload};

use fluvio_protocol::{Encoder, Decoder, Version};

use super::{
    SmartModuleMetadata,
    spec_v1::{SmartModuleSpecV1},
};

const V2_FORMAT: Version = 10;

#[cfg(feature = "smartmodule_validation")]
#[derive(thiserror::Error, Debug)]
pub enum ValidateSmartModuleWasmError {
    #[error("The provided WASM file is not valid: {0}")]
    BinaryParsingError(String),
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleSpec {
    pub meta: Option<SmartModuleMetadata>,
    #[cfg_attr(feature = "use_serde", serde(skip))]
    pub summary: Option<SmartModuleWasmSummary>, // only passed from SC to CLI
    pub wasm: SmartModuleWasm,
}

// custom encoding to handle prev version
impl Encoder for SmartModuleSpec {
    fn write_size(&self, version: Version) -> usize {
        if version < V2_FORMAT {
            //trace!("computing size for smart module spec v1");
            // just used for computing size
            let spec_v1 = SmartModuleSpecV1::default();
            let mut size = 0;
            size += spec_v1.input_kind.write_size(version);
            size += spec_v1.output_kind.write_size(version);
            size += spec_v1.source_code.write_size(version);
            size += self.wasm.write_size(version);
            size += spec_v1.parameters.write_size(version);
            size
        } else {
            let mut size = 0;
            size += self.meta.write_size(version);
            size += self.summary.write_size(version);
            size += self.wasm.write_size(version);
            size
        }
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        if version < V2_FORMAT {
            debug!("encoding for smart module spec v1");
            let spec_v1 = SmartModuleSpecV1::default();
            spec_v1.input_kind.encode(dest, version)?;
            spec_v1.output_kind.encode(dest, version)?;
            spec_v1.source_code.encode(dest, version)?;
            self.wasm.encode(dest, version)?;
            spec_v1.parameters.encode(dest, version)?;
        } else {
            self.meta.encode(dest, version)?;
            self.summary.encode(dest, version)?;
            self.wasm.encode(dest, version)?;
        }
        Ok(())
    }
}

impl Decoder for SmartModuleSpec {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: bytes::Buf,
    {
        if version < V2_FORMAT {
            debug!("decoding for smart module spec v1");
            let mut spec_v1 = SmartModuleSpecV1::default();
            spec_v1.decode(src, version)?;
            self.wasm = spec_v1.wasm;
        } else {
            self.meta.decode(src, version)?;
            self.summary.decode(src, version)?;
            self.wasm.decode(src, version)?;
        }

        Ok(())
    }
}

impl SmartModuleSpec {
    /// return fully qualified name given store key
    pub fn fqdn<'a>(&self, store_id: &'a str) -> Cow<'a, str> {
        if let Some(meta) = &self.meta {
            meta.package.fqdn().into()
        } else {
            Cow::from(store_id)
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
pub struct SmartModuleWasmSummary {
    pub wasm_length: u32,
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

    #[cfg(feature = "smartmodule_validation")]
    /// Validates a SmartModule's WASM payload to represent a valid WASM file
    /// in the binary format (*.wasm).
    pub fn validate_binary(mut data: &[u8]) -> Result<(), ValidateSmartModuleWasmError> {
        let mut parser = Parser::default();

        loop {
            match parser
                .parse(data, true)
                .map_err(|err| ValidateSmartModuleWasmError::BinaryParsingError(err.to_string()))?
            {
                // Given that file bytes are present, its not possible to meet
                // this state.
                Chunk::NeedMoreData(_) => unreachable!(),
                Chunk::Parsed { consumed, payload } => {
                    if matches!(&payload, Payload::End(_)) {
                        // Reaches the EOF with success. At this point the
                        // whole file has been read and no errors has occured.
                        return Ok(());
                    }

                    // Keeps track of parsing offset.
                    data = &data[consumed..];
                }
            };
        }
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

#[cfg(all(test, feature = "smartmodule_validation"))]
mod tests {
    use std::fs::read;

    use super::SmartModuleWasm;

    #[test]
    fn validates_smartmodule_wasm_binary() {
        let wasm_bytes =
            read("tests/sm_ok.wasm").expect("Failed to find 'sm_ok.wasm' file for tests");
        let result = SmartModuleWasm::validate_binary(wasm_bytes.as_slice());

        assert!(result.is_ok());
    }

    #[test]
    fn validate_input_to_be_wasm_file() {
        let wasm_bytes =
            read("tests/sm_k8_v1.yaml").expect("Failed to find 'sm_k8_v1.yaml' file for tests");
        let result = SmartModuleWasm::validate_binary(wasm_bytes.as_slice());

        assert!(result.is_err());
    }

    #[test]
    fn validates_smartmodule_wasm_binary_corrupt() {
        let wasm_bytes = read("tests/sm_need_more_data.wasm")
            .expect("Failed to find 'sm_need_more_data.wasm' file for tests");
        let result = SmartModuleWasm::validate_binary(wasm_bytes.as_slice());

        assert!(result.is_err());
    }
}
