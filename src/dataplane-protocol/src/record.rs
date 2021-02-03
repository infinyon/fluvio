use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::io::Error;
use std::io::ErrorKind;
use std::sync::Arc;

use content_inspector::{inspect, ContentType};
use log::{trace, warn};
use once_cell::sync::Lazy;

use crate::core::bytes::Buf;
use crate::core::bytes::BufExt;
use crate::core::bytes::BufMut;

use crate::core::Decoder;
use crate::core::DecoderVarInt;
use crate::core::Encoder;
use crate::core::EncoderVarInt;
use crate::core::Version;
use crate::derive::Decode;
use crate::derive::Encode;

use crate::batch::DefaultBatch;
use crate::Offset;

pub type DefaultRecord = Record<DefaultAsyncBuffer>;

/// maximum text to display
static MAX_STRING_DISPLAY: Lazy<usize> = Lazy::new(|| {
    let var_value = std::env::var("FLV_MAX_STRING_DISPLAY").unwrap_or_default();
    var_value.parse().unwrap_or(16384)
});

pub use file::*;

/// slice that can works in Async Context
pub trait AsyncBuffer {
    fn len(&self) -> usize;
}

pub trait Records {}

#[derive(Default)]
pub struct DefaultAsyncBuffer(Arc<Vec<u8>>);

impl DefaultAsyncBuffer {
    pub fn new<T>(val: T) -> Self
    where
        T: Into<Arc<Vec<u8>>>,
    {
        DefaultAsyncBuffer(val.into())
    }

    /*
    pub fn inner_value(self) -> Vec<u8> {
        *self.0.clone()
    }
    */

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if value is binary content
    pub fn is_binary(&self) -> bool {
        matches!(inspect(&self.0), ContentType::BINARY)
    }

    /// Describe value - return text, binary, or 0 bytes
    pub fn describe(&self) -> String {
        if self.is_binary() {
            format!("binary: ({} bytes)", self.len())
        } else {
            format!("text: '{}'", self)
        }
    }
}

impl AsRef<[u8]> for DefaultAsyncBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsyncBuffer for DefaultAsyncBuffer {
    fn len(&self) -> usize {
        self.len()
    }
}

impl Debug for DefaultAsyncBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let value = &self.0;
        if matches!(inspect(value), ContentType::BINARY) {
            write!(f, "values binary: ({} bytes)", self.len())
        } else if value.len() < *MAX_STRING_DISPLAY {
            write!(f, "{}", String::from_utf8_lossy(value))
        } else {
            write!(
                f,
                "{}...",
                String::from_utf8_lossy(&value[0..*MAX_STRING_DISPLAY])
            )
        }
    }
}

impl Display for DefaultAsyncBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let value = &self.0;
        if matches!(inspect(value), ContentType::BINARY) {
            write!(f, "binary: ({} bytes)", self.len())
        } else if value.len() < *MAX_STRING_DISPLAY {
            write!(f, "{}", String::from_utf8_lossy(value))
        } else {
            write!(
                f,
                "{}...",
                String::from_utf8_lossy(&value[0..*MAX_STRING_DISPLAY])
            )
        }
    }
}

impl From<String> for DefaultAsyncBuffer {
    fn from(value: String) -> Self {
        Self::new(value.into_bytes())
    }
}

impl From<Vec<u8>> for DefaultAsyncBuffer {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl<'a> From<&'a [u8]> for DefaultAsyncBuffer {
    fn from(bytes: &'a [u8]) -> Self {
        let value = bytes.to_owned();
        Self::new(value)
    }
}

impl Encoder for DefaultAsyncBuffer {
    fn write_size(&self, version: Version) -> usize {
        self.0.write_size(version)
    }

    fn encode<T>(&self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        self.0.encode(src, version)
    }
}

impl Decoder for DefaultAsyncBuffer {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("decoding default asyncbuffer");
        if let Some(ref mut buffer) = Arc::get_mut(&mut self.0) {
            buffer.decode(src, version)
        } else {
            Err(Error::new(
                ErrorKind::Other,
                "Can't decode buffer while cloning".to_string(),
            ))
        }
    }
}

#[derive(Default, Debug)]
pub struct RecordSet {
    pub batches: Vec<DefaultBatch>,
}

impl fmt::Display for RecordSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} batches", self.batches.len())
    }
}

impl RecordSet {
    pub fn add(mut self, batch: DefaultBatch) -> Self {
        self.batches.push(batch);
        self
    }
}

impl Decoder for RecordSet {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("raw buffer len: {}", src.remaining());
        let mut len: i32 = 0;
        len.decode(src, version)?;
        trace!("Record sets decoded content len: {}", len);

        if src.remaining() < len as usize {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "expected message len: {} but founded {}",
                    len,
                    src.remaining()
                ),
            ));
        }

        let mut buf = src.take(len as usize);

        let mut count = 0;
        while buf.remaining() > 0 {
            trace!(
                "decoding batches: {}, remaining bytes: {}",
                count,
                buf.remaining()
            );
            let mut batch = DefaultBatch::default();
            match batch.decode(&mut buf, version) {
                Ok(_) => self.batches.push(batch),
                Err(err) => match err.kind() {
                    ErrorKind::UnexpectedEof => {
                        warn!("not enough bytes for batch: {}", buf.remaining());
                        return Ok(());
                    }
                    _ => {
                        warn!("problem decoding batch: {}", err);
                        return Ok(());
                    }
                },
            }
            count += 1;
        }

        Ok(())
    }
}

impl Encoder for RecordSet {
    fn write_size(&self, version: Version) -> usize {
        self.batches
            .iter()
            .fold(4, |sum, val| sum + val.write_size(version))
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        trace!("Record set encoding");

        let mut out: Vec<u8> = Vec::new();

        for batch in &self.batches {
            trace!("encoding batch..");
            batch.encode(&mut out, version)?;
        }

        let length: i32 = out.len() as i32;
        trace!("Record Set encode len: {}", length);
        length.encode(dest, version)?;

        dest.put_slice(&out);
        Ok(())
    }
}

#[derive(Decode, Encode, Default, Debug)]
pub struct RecordHeader {
    attributes: i8,
    #[varint]
    timestamp_delta: i64,
    #[varint]
    offset_delta: Offset,
}

impl RecordHeader {
    pub fn set_offset_delta(&mut self, delta: Offset) {
        self.offset_delta = delta;
    }
}

#[derive(Default)]
pub struct Record<B>
where
    B: Default,
{
    pub preamble: RecordHeader,
    pub key: B,
    pub value: B,
    pub headers: i64,
}

impl<B> Record<B>
where
    B: Default,
{
    pub fn get_offset_delta(&self) -> Offset {
        self.preamble.offset_delta
    }

    pub fn get_value(&self) -> &B {
        &self.value
    }

    pub fn value(self) -> B {
        self.value
    }
}

impl<B> Debug for Record<B>
where
    B: AsyncBuffer + Debug + Default,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", &self.preamble)?;
        writeln!(f, "{:?}", &self.key)?;
        writeln!(f, "{:?}", &self.value)?;
        write!(f, "{:?}", &self.headers)
    }
}

impl<B> From<String> for Record<B>
where
    B: From<String> + Default,
{
    fn from(value: String) -> Self {
        Record {
            value: value.into(),
            ..Default::default()
        }
    }
}

impl<B> From<Vec<u8>> for Record<B>
where
    B: From<Vec<u8>> + Default,
{
    fn from(value: Vec<u8>) -> Self {
        Record {
            value: value.into(),
            ..Default::default()
        }
    }
}

impl<'a, B> From<&'a [u8]> for Record<B>
where
    B: From<&'a [u8]> + Default,
{
    fn from(slice: &'a [u8]) -> Self {
        Record {
            value: B::from(slice),
            ..Default::default()
        }
    }
}

impl<B> Encoder for Record<B>
where
    B: Encoder + Default,
{
    fn write_size(&self, version: Version) -> usize {
        let inner_size = self.preamble.write_size(version)
            + self.key.write_size(version)
            + self.value.write_size(version)
            + self.headers.var_write_size();
        let len: i64 = inner_size as i64;
        len.var_write_size() + inner_size
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        let mut out: Vec<u8> = Vec::new();
        self.preamble.encode(&mut out, version)?;
        self.key.encode(&mut out, version)?;
        self.value.encode(&mut out, version)?;
        self.headers.encode_varint(&mut out)?;
        let len: i64 = out.len() as i64;
        trace!("record encode as {} bytes", len);
        len.encode_varint(dest)?;
        dest.put_slice(&out);
        Ok(())
    }
}

impl<B> Decoder for Record<B>
where
    B: Decoder,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("decoding record");
        let mut len: i64 = 0;
        len.decode_varint(src)?;

        trace!("record contains: {} bytes", len);

        if (src.remaining() as i64) < len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enought for record",
            ));
        }
        self.preamble.decode(src, version)?;
        trace!("offset delta: {}", self.preamble.offset_delta);
        self.key.decode(src, version)?;
        self.value.decode(src, version)?;
        self.headers.decode_varint(src)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::io::Cursor;
    use std::io::Error as IoError;

    use crate::core::Decoder;
    use crate::core::Encoder;
    use crate::record::DefaultRecord;

    #[test]
    fn test_decode_encode_record() -> Result<(), IoError> {
        
        /* Below is how you generate the vec<u8> for the `data` varible below.
        let mut record = DefaultRecord::from(String::from("dog"));
        record.preamble.set_offset_delta(1);
        let mut out = vec![];
        record.encode(&mut out, 0)?;
        println!("ENCODED: {:#x?}", out);
        */
        
        let data = [
            0x1e,
            0x0,
            0x0,
            0x2,
            0x0,
            0x0,
            0x0,
            0x0,
            0x0,
            0x0,
            0x0,
            0x3,
            0x64,
            0x6f,
            0x67,
            0x0,
        ];

        let record = DefaultRecord::decode_from(&mut Cursor::new(&data), 0)?;
        assert_eq!(record.as_bytes(0)?.len(), data.len());

        assert_eq!(record.write_size(0), data.len());
        println!("offset_delta: {:?}", record.get_offset_delta());
        assert_eq!(record.get_offset_delta(), 1);

        let value = record.value.as_ref();
        assert_eq!(value.len(), 3);
        assert_eq!(value[0], 0x64);

        Ok(())
    }

    /// test decoding of records when one of the batch was truncated
    #[test]
    fn test_decode_batch_truncation() {
        use super::RecordSet;
        use crate::batch::DefaultBatch;
        use crate::record::DefaultRecord;

        fn create_batch() -> DefaultBatch {
            let record: DefaultRecord = vec![0x74, 0x65, 0x73, 0x74].into();
            let mut batch = DefaultBatch::default();
            batch.records.push(record);
            batch
        }

        // add 3 batches
        let batches = RecordSet::default()
            .add(create_batch())
            .add(create_batch())
            .add(create_batch());

        const TRUNCATED: usize = 10;

        let mut bytes = batches.as_bytes(0).expect("bytes");

        let original_len = bytes.len();
        let _ = bytes.split_off(original_len - TRUNCATED); // truncate record sets
        let body = bytes.split_off(4); // split off body so we can manipulate len

        let new_len = (original_len - TRUNCATED - 4) as i32;
        let mut out = vec![];
        new_len.encode(&mut out, 0).expect("encoding");
        out.extend_from_slice(&body);

        assert_eq!(out.len(), original_len - TRUNCATED);

        println!("decoding...");
        let decoded_batches = RecordSet::decode_from(&mut Cursor::new(out), 0).expect("decoding");
        assert_eq!(decoded_batches.batches.len(), 2);
    }
}

mod file {

    use std::fmt;
    use std::io::Error as IoError;

    use log::trace;
    use bytes::BufMut;
    use bytes::BytesMut;

    use fluvio_future::file_slice::AsyncFileSlice;
    use crate::core::bytes::Buf;
    use crate::core::Decoder;
    use crate::core::Encoder;
    use crate::core::Version;
    use crate::store::FileWrite;
    use crate::store::StoreValue;

    #[derive(Default, Debug)]
    pub struct FileRecordSet(AsyncFileSlice);

    impl fmt::Display for FileRecordSet {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "pos: {} len: {}", self.position(), self.len())
        }
    }

    impl FileRecordSet {
        pub fn position(&self) -> u64 {
            self.0.position()
        }

        pub fn len(&self) -> usize {
            self.0.len() as usize
        }

        pub fn raw_slice(&self) -> AsyncFileSlice {
            self.0.clone()
        }
    }

    impl From<AsyncFileSlice> for FileRecordSet {
        fn from(slice: AsyncFileSlice) -> Self {
            Self(slice)
        }
    }

    impl Encoder for FileRecordSet {
        fn write_size(&self, _version: Version) -> usize {
            self.len() + 4 // include header
        }

        fn encode<T>(&self, _src: &mut T, _version: Version) -> Result<(), IoError>
        where
            T: BufMut,
        {
            unimplemented!("file slice cannot be encoded in the ButMut")
        }
    }

    impl Decoder for FileRecordSet {
        fn decode<T>(&mut self, _src: &mut T, _version: Version) -> Result<(), IoError>
        where
            T: Buf,
        {
            unimplemented!("file slice cannot be decoded in the ButMut")
        }
    }

    impl FileWrite for FileRecordSet {
        fn file_encode(
            &self,
            dest: &mut BytesMut,
            data: &mut Vec<StoreValue>,
            version: Version,
        ) -> Result<(), IoError> {
            let len: i32 = self.len() as i32;
            trace!("KfFileRecordSet encoding file slice len: {}", len);
            len.encode(dest, version)?;
            let bytes = dest.split_to(dest.len()).freeze();
            data.push(StoreValue::Bytes(bytes));
            data.push(StoreValue::FileSlice(self.raw_slice()));
            Ok(())
        }
    }
}
