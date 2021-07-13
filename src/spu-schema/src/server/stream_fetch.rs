//!
//! # Continuous Fetch
//!
//! Stream records to client
//!
use std::io::{Error, ErrorKind};
use std::fmt::Debug;
use std::marker::PhantomData;

use bytes::{BufMut, Buf};
use dataplane::core::{Encoder, Version};
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
pub const WASM_MODULE_V2_API: i16 = 12;

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
    #[fluvio(min_version = 12)]
    pub wasm_payload: Option<SmartStreamPayload>,
    pub data: PhantomData<R>,
}

impl<R> Request for StreamFetchRequest<R>
where
    R: Debug + Decoder + Encoder,
{
    const API_KEY: u16 = SpuServerApiKey::StreamFetch as u16;
    const DEFAULT_API_VERSION: i16 = WASM_MODULE_V2_API;
    type Response = StreamFetchResponse<R>;
}

/// The request payload when using a Consumer SmartStream.
///
/// This includes the WASM content as well as the type of SmartStream being used.
/// It also carries any data that is required for specific types of SmartStreams.
#[derive(Debug, Default, Encode, Decode)]
pub struct SmartStreamPayload {
    pub wasm: SmartStreamWasm,
    pub kind: SmartStreamKind,
}

/// Indicates the type of SmartStream as well as any special data required
#[derive(Debug)]
pub enum SmartStreamKind {
    Filter,
    Map,
}

impl Default for SmartStreamKind {
    fn default() -> Self {
        Self::Filter
    }
}

impl Encoder for SmartStreamKind {
    fn write_size(&self, version: Version) -> usize {
        0u8.write_size(version)
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        match self {
            Self::Filter => 0u8.encode(dest, version),
            Self::Map => 1u8.encode(dest, version),
        }
    }
}

impl Decoder for SmartStreamKind {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut tag = 0u8;
        tag.decode(src, version)?;
        match tag {
            0u8 => {
                *self = Self::Filter;
            }
            1u8 => {
                *self = Self::Map;
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("failed to decode SmartStreamKind, invalid tag '{}'", tag),
                ));
            }
        }
        Ok(())
    }
}

/// Different possible representations of WASM modules.
///
/// In a fetch request, a WASM module may be given directly in the request
/// as raw bytes.
///
// TODO ... or, it may be named and selected from the WASM store.
#[derive(Debug)]
pub enum SmartStreamWasm {
    Raw(Vec<u8>),
    // TODO implement named WASM modules once we have a WASM store
    // Url(String),
}

impl Default for SmartStreamWasm {
    fn default() -> Self {
        Self::Raw(Vec::new())
    }
}

impl Encoder for SmartStreamWasm {
    fn write_size(&self, version: Version) -> usize {
        match self {
            Self::Raw(value) => 0u8.write_size(version) + value.write_size(version),
        }
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        match self {
            Self::Raw(bytes) => {
                0u8.encode(dest, version)?;
                bytes.encode(dest, version)?;
            }
        }
        Ok(())
    }
}

impl Decoder for SmartStreamWasm {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut tag = 0u8;
        tag.decode(src, version)?;
        match tag {
            0u8 => {
                let mut bytes = Vec::default();
                bytes.decode(src, version)?;
                *self = Self::Raw(bytes);
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("failed to decode SmartStreamWasm, invalid tag '{}'", tag),
                ));
            }
        }
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_smartstreamkind() {
        let mut dest = Vec::new();
        let value: SmartStreamKind = SmartStreamKind::Filter;
        value.encode(&mut dest, 0).expect("should encode");
        assert_eq!(dest.len(), 1);
        assert_eq!(dest[0], 0x00);
    }

    #[test]
    fn test_decode_smartstreamkind() {
        let bytes = vec![0x01];
        let mut value: SmartStreamKind = Default::default();
        value
            .decode(&mut std::io::Cursor::new(bytes), 0)
            .expect("should decode");
        assert!(matches!(value, SmartStreamKind::Map));
    }

    #[test]
    fn test_encode_smartstreamwasm() {
        let mut dest = Vec::new();
        let value: SmartStreamWasm = SmartStreamWasm::Raw(vec![0xde, 0xad, 0xbe, 0xef]);
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
    fn test_decode_smartstreamwasm() {
        let bytes = vec![0x00, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef];
        let mut value: SmartStreamWasm = Default::default();
        value
            .decode(&mut std::io::Cursor::new(bytes), 0)
            .expect("should decode");
        let inner = match value {
            SmartStreamWasm::Raw(inner) => inner,
            #[allow(unreachable_patterns)]
            _ => panic!("should decode to SmartStreamWasm::Raw"),
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
            wasm_payload: Some(SmartStreamPayload {
                kind: SmartStreamKind::Filter,
                wasm: SmartStreamWasm::Raw(vec![0xde, 0xad, 0xbe, 0xef]),
            }),
            ..Default::default()
        };
        value.encode(&mut dest, 12).expect("should encode");
        let expected = vec![
            0x00, 0x03, 0x6f, 0x6e, 0x65, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef, 0x00,
        ];
        assert_eq!(dest, expected);
    }

    #[test]
    fn test_decode_stream_fetch_request() {
        let bytes = vec![
            0x00, 0x03, 0x6f, 0x6e, 0x65, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef, 0x00,
        ];
        let mut value = DefaultStreamFetchRequest::default();
        value.decode(&mut std::io::Cursor::new(bytes), 12).unwrap();
        assert_eq!(value.topic, "one");
        assert_eq!(value.partition, 3);
        let smartstream = match value.wasm_payload {
            Some(wasm) => wasm,
            _ => panic!("should have smartstreeam payload"),
        };
        let wasm = match smartstream.wasm {
            SmartStreamWasm::Raw(wasm) => wasm,
            #[allow(unreachable_patterns)]
            _ => panic!("should be SmartStreamWasm::Raw"),
        };
        assert_eq!(wasm, vec![0xde, 0xad, 0xbe, 0xef]);
        assert!(matches!(smartstream.kind, SmartStreamKind::Filter));
    }
}
