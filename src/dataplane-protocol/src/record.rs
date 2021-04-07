use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::io::Error;
use std::io::ErrorKind;

use content_inspector::{inspect, ContentType};
use log::{trace, warn};
use once_cell::sync::Lazy;

use crate::core::bytes::Bytes;
use crate::core::bytes::Buf;
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

/// slice that can works in Async Context
pub trait AsyncBuffer {
    fn len(&self) -> usize;
}

pub trait Records {}

#[derive(Default)]
pub struct DefaultAsyncBuffer(Bytes);

impl DefaultAsyncBuffer {
    pub fn new<T>(val: T) -> Self
    where
        T: Into<Bytes>,
    {
        DefaultAsyncBuffer(val.into())
    }

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
        self.0.as_ref()
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
        let len = self.0.len() as i64;
        self.0.iter().fold(len.var_write_size(), |sum, val| {
            sum + val.write_size(version)
        })
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        let len: i64 = self.0.len() as i64;
        len.encode_varint(dest)?;
        for v in self.0.iter() {
            v.encode(dest, version)?;
        }
        Ok(())
    }
}

impl Decoder for DefaultAsyncBuffer {
    fn decode<T>(&mut self, src: &mut T, _: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("decoding default asyncbuffer");

        let mut len: i64 = 0;
        len.decode_varint(src)?;
        let len = len as usize;

        // Take `len` bytes from `src` and put them into a new BytesMut buffer
        let slice = src.take(len);
        let mut bytes_mut = bytes::BytesMut::with_capacity(len);
        bytes_mut.put(slice);

        // Replace the inner Bytes buffer of this DefaultAsyncBuffer
        self.0 = bytes_mut.freeze();
        Ok(())
    }
}

/// Represents sets of batches in storage
//  It is written consequently with len as prefix
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

    /// this is next offset to be fetched
    pub fn last_offset(&self) -> Option<Offset> {
        self.batches
            .last()
            .map(|batch| batch.computed_last_offset())
    }

    /// total records
    pub fn total_records(&self) -> usize {
        self.batches
            .iter()
            .map(|batches| batches.records().len())
            .sum()
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

    pub fn offset_delta(&self) -> Offset {
        self.offset_delta
    }
}

#[derive(Default)]
pub struct Record<B>
where
    B: Default,
{
    pub preamble: RecordHeader,
    pub key: Option<B>,
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

    /// add offset delta with new relative base offset
    pub fn add_base_offset(&mut self, relative_base_offset: Offset) {
        self.preamble.offset_delta += relative_base_offset;
    }

    /// Returns a reference to the inner value
    pub fn value(&self) -> &B {
        &self.value
    }

    /// Returns a reference to the inner key if it exists
    pub fn key(&self) -> Option<&B> {
        self.key.as_ref()
    }

    /// Consumes this record, returning the inner value
    pub fn into_value(self) -> B {
        self.value
    }

    /// Consumes this record, returning the inner key if it esists
    pub fn into_key(self) -> Option<B> {
        self.key
    }
}

impl DefaultRecord {
    pub fn new<V>(value: V) -> Self
    where
        V: Into<Vec<u8>>,
    {
        DefaultRecord {
            value: DefaultAsyncBuffer::new(value.into()),
            ..Default::default()
        }
    }

    pub fn new_key_value<K, V>(key: K, value: V) -> Self
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        DefaultRecord {
            key: Some(DefaultAsyncBuffer::new(key.into())),
            value: DefaultAsyncBuffer::new(value.into()),
            ..Default::default()
        }
    }
}

impl<B: Default> From<(Option<B>, B)> for Record<B> {
    fn from((key, value): (Option<B>, B)) -> Self {
        Record {
            key,
            value,
            ..Default::default()
        }
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
        let data = [0x12, 0x0, 0x0, 0x2, 0x0, 0x6, 0x64, 0x6f, 0x67, 0x0];

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
            let value = vec![0x74, 0x65, 0x73, 0x74];
            let record = DefaultRecord::new(value);
            let mut batch = DefaultBatch::default();
            batch.add_record(record);
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

    #[test]
    fn test_key_value_encoding() {
        let key = "KKKKKKKKKK".to_string();
        let value = "VVVVVVVVVV".to_string();
        let record = DefaultRecord::new_key_value(key, value);

        let mut encoded = Vec::new();
        record.encode(&mut encoded, 0).unwrap();
        let decoded = DefaultRecord::decode_from(&mut Cursor::new(encoded), 0).unwrap();

        let record_key = record.key.unwrap();
        let decoded_key = decoded.key.unwrap();
        assert_eq!(record_key.0.as_ref(), decoded_key.0.as_ref());
        assert_eq!(record.value.0.as_ref(), decoded.value.0.as_ref());
    }

    // Test Specification:
    //
    // A record was encoded and written to a file, using the following code:
    //
    // ```rust
    // use fluvio_dataplane_protocol::record::{DefaultRecord, DefaultAsyncBuffer};
    // use fluvio_protocol::Encoder;
    // let value = "VVVVVVVVVV".to_string();
    // let record = DefaultRecord {
    //     key: DefaultAsyncBuffer::default(),
    //     value: DefaultAsyncBuffer::new(value.into_bytes()),
    //     ..Default::default()
    // };
    // let mut encoded = Vec::new();
    // record.encode(&mut encoded, 0);
    // ```
    //
    // This was back when records defined keys as `key: B` rather than `key: Option<B>`.
    //
    // It just so happens that our public API never allowed any records to be sent with
    // any contents in the `key` field, so what was sent over the wire was a buffer whose
    // length was zero, encoded as a single zero `0x00` (for "length-zero buffer").
    //
    // In the new `key: Option<B>` encoding, a key is encoded with a tag for
    // Some or None, with 0x00 representing None and 0x01 representing Some.
    // So, when reading old records, the 0x00 "length encoding" will be re-interpreted
    // as the 0x00 "None encoding". Since all old keys were empty, this should work for
    // all old records _in practice_. This will not work if any record is decoded which
    // artificially was given contents in the key field.
    #[test]
    fn test_decode_old_record_empty_key() {
        let old_encoded = std::fs::read("./tests/test_old_record_empty_key.bin").unwrap();
        let decoded = DefaultRecord::decode_from(&mut Cursor::new(old_encoded), 0).unwrap();
        assert_eq!(
            std::str::from_utf8(decoded.value.0.as_ref()).unwrap(),
            "VVVVVVVVVV"
        );
        assert!(decoded.key.is_none());
    }
}

#[cfg(feature = "file")]
pub use file::*;

#[cfg(feature = "file")]
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
            // write total len
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
