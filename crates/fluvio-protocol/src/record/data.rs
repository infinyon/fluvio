use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::io::Error;
use std::io::ErrorKind;
use std::str::Utf8Error;

use bytes::Bytes;
use bytes::BytesMut;
use content_inspector::{inspect, ContentType};
use fluvio_types::PartitionId;
use tracing::{trace, warn};
use once_cell::sync::Lazy;

use bytes::Buf;
use bytes::BufMut;

use crate::{Encoder, Decoder};
use crate::DecoderVarInt;
use crate::EncoderVarInt;
use crate::Version;

use super::batch::BatchRecords;
use super::batch::MemoryRecords;
use super::batch::NO_TIMESTAMP;
use super::batch::RawRecords;
use super::batch::Batch;
use super::Offset;

use fluvio_compression::CompressionError;
use fluvio_types::Timestamp;

/// maximum text to display
static MAX_STRING_DISPLAY: Lazy<usize> = Lazy::new(|| {
    let var_value = std::env::var("FLV_MAX_STRING_DISPLAY").unwrap_or_default();
    var_value.parse().unwrap_or(16384)
});

/// A key for determining which partition a record should be sent to.
///
/// This type is used to support conversions from any other type that
/// may be converted to a `Vec<u8>`, while still allowing the ability
/// to explicitly state that a record may have no key (`RecordKey::NULL`).
///
/// # Examples
///
/// ```
/// # use fluvio_protocol::record::RecordKey;
/// let key = RecordKey::NULL;
/// let key: RecordKey = "Hello, world!".into();
/// let key: RecordKey = String::from("Hello, world!").into();
/// let key: RecordKey = vec![1, 2, 3, 4].into();
/// ```
#[derive(Hash)]
pub struct RecordKey(RecordKeyInner);

impl RecordKey {
    pub const NULL: Self = Self(RecordKeyInner::Null);

    fn into_option(self) -> Option<RecordData> {
        match self.0 {
            RecordKeyInner::Key(key) => Some(key),
            RecordKeyInner::Null => None,
        }
    }

    #[doc(hidden)]
    pub fn from_option(key: Option<RecordData>) -> Self {
        let inner = match key {
            Some(key) => RecordKeyInner::Key(key),
            None => RecordKeyInner::Null,
        };
        Self(inner)
    }
}

#[derive(Hash)]
enum RecordKeyInner {
    Null,
    Key(RecordData),
}

impl<K: Into<Vec<u8>>> From<K> for RecordKey {
    fn from(k: K) -> Self {
        Self(RecordKeyInner::Key(RecordData::from(k)))
    }
}

/// A type containing the data contents of a Record.
///
/// The `RecordData` type provides useful conversions for
/// constructing it from any type that may convert into a `Vec<u8>`.
/// This is the basis for flexible APIs that allow users to supply
/// various different argument types as record contents. See
/// [the Producer API] as an example.
///
/// [the Producer API]: https://docs.rs/fluvio/producer/TopicProducer::send
#[derive(Clone, Default, Eq, PartialEq, Hash)]
pub struct RecordData(Bytes);

impl RecordData {
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
            format!("text: '{self}'")
        }
    }

    // as string slice
    pub fn as_str(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(self.as_ref())
    }
}

impl<V: Into<Vec<u8>>> From<V> for RecordData {
    fn from(value: V) -> Self {
        Self(Bytes::from(value.into()))
    }
}

impl AsRef<[u8]> for RecordData {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Debug for RecordData {
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

impl Display for RecordData {
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

impl Encoder for RecordData {
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

impl Decoder for RecordData {
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
        let mut bytes = BytesMut::with_capacity(len);
        bytes.put(slice);

        // Replace the inner Bytes buffer of this RecordData
        self.0 = bytes.freeze();
        Ok(())
    }
}

/// Represents sets of batches in storage
//  It is written consequently with len as prefix
#[derive(Default, Debug)]
pub struct RecordSet<R = MemoryRecords> {
    pub batches: Vec<Batch<R>>,
}

impl TryFrom<RecordSet> for RecordSet<RawRecords> {
    type Error = CompressionError;
    fn try_from(set: RecordSet) -> Result<Self, Self::Error> {
        let batches: Result<Vec<_>, _> = set
            .batches
            .into_iter()
            .map(|batch| batch.try_into())
            .collect();
        Ok(Self { batches: batches? })
    }
}

impl fmt::Display for RecordSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} batches", self.batches.len())
    }
}

impl<R: BatchRecords> RecordSet<R> {
    pub fn add(mut self, batch: Batch<R>) -> Self {
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
            .map(|batches| batches.records_len())
            .sum()
    }

    /// return base offset
    pub fn base_offset(&self) -> Offset {
        self.batches
            .first()
            .map(|batches| batches.base_offset)
            .unwrap_or_else(|| -1)
    }
}

impl<R: BatchRecords> Decoder for RecordSet<R> {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!(len = src.remaining(), "raw buffer size");
        let mut len: i32 = 0;
        len.decode(src, version)?;
        trace!(len, "Record sets decoded content");

        if src.remaining() < len as usize {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "expected message len: {} but found {}",
                    len,
                    src.remaining()
                ),
            ));
        }

        let mut buf = src.take(len as usize);

        let mut count = 0;
        while buf.remaining() > 0 {
            trace!(count, remaining = buf.remaining(), "decoding batches");
            let mut batch = Batch::default();
            match batch.decode(&mut buf, version) {
                Ok(_) => self.batches.push(batch),
                Err(err) => match err.kind() {
                    ErrorKind::UnexpectedEof => {
                        warn!(
                            len,
                            remaining = buf.remaining(),
                            version,
                            count,
                            "not enough bytes for decoding batch from recordset"
                        );
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

impl<R: BatchRecords> Encoder for RecordSet<R> {
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

impl<R: Clone> Clone for RecordSet<R> {
    fn clone(&self) -> Self {
        Self {
            batches: self.batches.clone(),
        }
    }
}

#[derive(Decoder, Default, Encoder, Debug, Clone)]
pub struct RecordHeader {
    attributes: i8,
    #[varint]
    timestamp_delta: Timestamp,
    #[varint]
    offset_delta: Offset,
}

impl RecordHeader {
    #[inline]
    pub fn get_offset_delta(&self) -> Offset {
        self.offset_delta
    }

    pub fn set_offset_delta(&mut self, delta: Offset) {
        self.offset_delta = delta;
    }

    #[inline]
    pub fn offset_delta(&self) -> Offset {
        self.offset_delta
    }

    pub fn set_timestamp_delta(&mut self, delta: Timestamp) {
        self.timestamp_delta = delta;
    }

    pub fn add_base_offset(&mut self, relative_base_offset: Offset) {
        self.offset_delta += relative_base_offset;
    }
}

#[derive(Default, Clone)]
pub struct Record<B = RecordData> {
    pub preamble: RecordHeader,
    pub key: Option<B>,
    pub value: B,
    pub headers: i64,
}

impl<B: Default> Record<B> {
    /// return reference to header
    pub fn get_header(&self) -> &RecordHeader {
        &self.preamble
    }

    /// return mutable reference to header
    pub fn get_mut_header(&mut self) -> &mut RecordHeader {
        &mut self.preamble
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

    /// Consumes this record, returning the inner key if it exists
    pub fn into_key(self) -> Option<B> {
        self.key
    }
}

impl Record {
    pub fn new<V>(value: V) -> Self
    where
        V: Into<RecordData>,
    {
        Record {
            value: value.into(),
            ..Default::default()
        }
    }

    pub fn new_key_value<K, V>(key: K, value: V) -> Self
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
    {
        let key = key.into().into_option();
        Record {
            key,
            value: value.into(),
            ..Default::default()
        }
    }

    pub fn timestamp_delta(&self) -> Timestamp {
        self.preamble.timestamp_delta
    }
}

impl<K, V> From<(K, V)> for Record
where
    K: Into<RecordKey>,
    V: Into<RecordData>,
{
    fn from((key, value): (K, V)) -> Self {
        Self::new_key_value(key, value)
    }
}

impl<B: Debug> Debug for Record<B> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Record")
            .field("preamble", &self.preamble)
            .field("key", &self.key)
            .field("value", &self.value)
            .field("headers", &self.headers)
            .finish()
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
                "not enough for record",
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

use Record as DefaultRecord;

/// Record that can be used by Consumer which needs access to metadata
pub struct ConsumerRecord<B = DefaultRecord> {
    /// The offset of this Record into its partition
    pub offset: i64,
    /// The partition where this Record is stored
    pub partition: PartitionId,
    /// The Record contents
    pub record: B,
    /// Timestamp base of batch in which the records is present
    pub(crate) timestamp_base: Timestamp,
}

impl<B> ConsumerRecord<B> {
    /// The offset from the initial offset for a given stream.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// The partition where this Record is stored.
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the inner representation of the Record
    pub fn into_inner(self) -> B {
        self.record
    }

    /// Returns a ref to the inner representation of the Record
    pub fn inner(&self) -> &B {
        &self.record
    }
}

impl ConsumerRecord<DefaultRecord> {
    /// Returns the contents of this Record's key, if it exists
    pub fn key(&self) -> Option<&[u8]> {
        self.record.key().map(|it| it.as_ref())
    }

    /// Returns the contents of this Record's value
    pub fn value(&self) -> &[u8] {
        self.record.value().as_ref()
    }
    /// Return the timestamp of the Record
    pub fn timestamp(&self) -> Timestamp {
        if self.timestamp_base <= 0 {
            NO_TIMESTAMP
        } else {
            self.timestamp_base + self.record.timestamp_delta()
        }
    }
}

impl AsRef<[u8]> for ConsumerRecord<DefaultRecord> {
    fn as_ref(&self) -> &[u8] {
        self.value()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;
    use std::io::Error as IoError;

    use crate::core::Decoder;
    use crate::core::Encoder;
    use crate::record::Record;

    #[test]
    fn test_decode_encode_record() -> Result<(), IoError> {
        /* Below is how you generate the vec<u8> for the `data` variable below.
        let mut record = Record::from(String::from("dog"));
        record.preamble.set_offset_delta(1);
        let mut out = vec![];
        record.encode(&mut out, 0)?;
        println!("ENCODED: {:#x?}", out);
        */
        let data = [0x12, 0x0, 0x0, 0x2, 0x0, 0x6, 0x64, 0x6f, 0x67, 0x0];

        let record = Record::<RecordData>::decode_from(&mut Cursor::new(&data), 0)?;
        assert_eq!(record.as_bytes(0)?.len(), data.len());

        assert_eq!(record.write_size(0), data.len());
        println!("offset_delta: {:?}", record.get_header().get_offset_delta());
        assert_eq!(record.get_header().get_offset_delta(), 1);

        let value = record.value.as_ref();
        assert_eq!(value.len(), 3);
        assert_eq!(value[0], 0x64);

        Ok(())
    }

    /// test decoding of records when one of the batch was truncated
    #[test]
    fn test_decode_batch_truncation() {
        use super::RecordSet;
        use super::super::batch::Batch;
        use super::super::Record;

        fn create_batch() -> Batch {
            let value = vec![0x74, 0x65, 0x73, 0x74];
            let record = Record::new(value);
            let mut batch = Batch::default();
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
        let decoded_batches =
            RecordSet::<MemoryRecords>::decode_from(&mut Cursor::new(out), 0).expect("decoding");
        assert_eq!(decoded_batches.batches.len(), 2);
    }

    #[test]
    fn test_key_value_encoding() {
        let key = "KKKKKKKKKK".to_string();
        let value = "VVVVVVVVVV".to_string();
        let record = Record::new_key_value(key, value);

        let mut encoded = Vec::new();
        record.encode(&mut encoded, 0).unwrap();
        let decoded = Record::<RecordData>::decode_from(&mut Cursor::new(encoded), 0).unwrap();

        let record_key = record.key.unwrap();
        let decoded_key = decoded.key.unwrap();
        assert_eq!(record_key.as_ref(), decoded_key.as_ref());
        assert_eq!(record.value.as_ref(), decoded.value.as_ref());
    }

    // Test Specification:
    //
    // A record was encoded and written to a file, using the following code:
    //
    // ```rust
    // use fluvio_dataplane_protocol::record::{Record, DefaultAsyncBuffer};
    // use fluvio_protocol::Encoder;
    // let value = "VVVVVVVVVV".to_string();
    // let record = Record {
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
        let decoded = Record::<RecordData>::decode_from(&mut Cursor::new(old_encoded), 0).unwrap();
        assert_eq!(
            std::str::from_utf8(decoded.value.0.as_ref()).unwrap(),
            "VVVVVVVVVV"
        );
        assert!(decoded.key.is_none());
    }

    #[test]
    fn test_consumer_record_no_timestamp() {
        let record = ConsumerRecord::<Record<RecordData>> {
            timestamp_base: NO_TIMESTAMP,
            offset: 0,
            partition: 0,
            record: Default::default(),
        };

        assert_eq!(record.timestamp(), NO_TIMESTAMP);
        let record = ConsumerRecord::<Record<RecordData>> {
            timestamp_base: 0,
            offset: 0,
            partition: 0,
            record: Default::default(),
        };
        assert_eq!(record.timestamp(), NO_TIMESTAMP);
    }

    #[test]
    fn test_consumer_record_timestamp() {
        let record = ConsumerRecord::<Record<RecordData>> {
            timestamp_base: 1_000_000_000,
            offset: 0,
            partition: 0,
            record: Default::default(),
        };

        assert_eq!(record.timestamp(), 1_000_000_000);
        let mut memory_record = Record::<RecordData>::default();
        memory_record.preamble.timestamp_delta = 800;
        let record = ConsumerRecord::<Record<RecordData>> {
            timestamp_base: 1_000_000_000,
            record: memory_record,
            offset: 0,
            partition: 0,
        };
        assert_eq!(record.timestamp(), 1_000_000_800);
    }
}
