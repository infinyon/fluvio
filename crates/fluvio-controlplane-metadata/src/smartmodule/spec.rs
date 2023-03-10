//!
//! # SmartModule Spec
//!
use std::borrow::Cow;
use std::io::Error as IoError;

use bytes::BufMut;
use tracing::debug;

use fluvio_protocol::{ByteBuf, Encoder, Decoder, Version};

use super::{
    SmartModuleMetadata,
    spec_v1::{SmartModuleSpecV1},
};

const V2_FORMAT: Version = 10;

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
            //trace!("computing size for smartmodule spec v1");
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
            debug!("encoding for smartmodule spec v1");
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
            debug!("decoding for smartmodule spec v1");
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
    pub payload: ByteBuf,
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
            payload: ByteBuf::from(payload),
            format: SmartModuleWasmFormat::Binary,
        }
    }
}

#[cfg(feature = "smartmodule")]
impl SmartModuleWasm {
    /// Create SmartModule from uncompressed Wasm format
    pub fn from_raw_wasm_bytes(raw_payload: &[u8]) -> std::io::Result<Self> {
        use std::io::Read;
        use flate2::{Compression, bufread::GzEncoder};

        let mut encoder = GzEncoder::new(raw_payload, Compression::default());
        let mut buffer = Vec::with_capacity(raw_payload.len());
        encoder.read_to_end(&mut buffer)?;

        Ok(Self::from_compressed_gzip(buffer))
    }

    pub fn as_raw_wasm(&self) -> Result<Vec<u8>, IoError> {
        use std::io::Read;
        use flate2::bufread::GzDecoder;

        let mut wasm = Vec::with_capacity(self.payload.len());
        let mut decoder = GzDecoder::new(&**self.payload);
        decoder.read_to_end(&mut wasm)?;
        Ok(wasm)
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartModuleWasmFormat {
    #[default]
    #[cfg_attr(feature = "use_serde", serde(rename = "BINARY"))]
    #[fluvio(tag = 0)]
    Binary,
    #[cfg_attr(feature = "use_serde", serde(rename = "TEXT"))]
    #[fluvio(tag = 1)]
    Text,
}

#[cfg(feature = "use_serde")]
mod base64 {
    use std::ops::Deref;

    use serde::{Serialize, Deserialize};
    use serde::{Deserializer, Serializer};
    use base64::Engine;

    use fluvio_protocol::ByteBuf;

    pub fn serialize<S>(bytebuf: &ByteBuf, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let base64 = base64::engine::general_purpose::STANDARD.encode(bytebuf.deref());
        String::serialize(&base64, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ByteBuf, D::Error> {
        let b64 = String::deserialize(d)?;
        let bytes: Vec<u8> = base64::engine::general_purpose::STANDARD
            .decode(b64.as_bytes())
            .map_err(serde::de::Error::custom)?;
        let bytebuf = ByteBuf::from(bytes);

        Ok(bytebuf)
    }
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "smartmodule")]
    #[test]
    fn test_wasm_zip_unzip() {
        use super::*;

        //given
        let payload = b"test wasm";

        //when
        let wasm = SmartModuleWasm::from_raw_wasm_bytes(payload).expect("created wasm");
        let unzipped = wasm.as_raw_wasm().expect("unzipped wasm");

        //then
        assert_eq!(payload, unzipped.as_slice());
    }
}
