use std::io::Error;
use std::mem::size_of;
use std::fmt::Debug;
use bytes::Bytes;
use fluvio_compression::CompressionError;
use fluvio_types::PartitionId;
use tracing::trace;

use fluvio_compression::Compression;
use fluvio_types::Timestamp;

use crate::bytes::Buf;
use crate::bytes::BufMut;
use crate::{Decoder, Encoder};
use crate::Version;

use super::ConsumerRecord;
use super::Record;
use super::Offset;
use super::Size;

pub const COMPRESSION_CODEC_MASK: i16 = 0x07;
pub const NO_TIMESTAMP: i64 = -1;

pub trait BatchRecords: Default + Debug + Encoder + Decoder + Send + Sync {
    /// how many bytes does record wants to process
    #[deprecated]
    fn remainder_bytes(&self, remainder: usize) -> usize {
        remainder
    }
}

/// A type describing in-memory records
pub type MemoryRecords = Vec<Record>;

/// A type describing Raw records
/// This structs decodes and encode its bytes as it is. Just the raw bytes of its internal vector.
/// When decoding, please be sure that your src buffer have the exact number of bytes.
#[derive(Debug, Default, Clone)]
pub struct RawRecords(pub Bytes);

impl Encoder for RawRecords {
    fn write_size(&self, _version: Version) -> usize {
        self.0.len()
    }

    fn encode<T: BufMut>(&self, buf: &mut T, _version: Version) -> Result<(), Error> {
        buf.put_slice(&self.0);
        Ok(())
    }
}

impl Decoder for RawRecords {
    fn decode<T: Buf>(&mut self, buf: &mut T, _version: Version) -> Result<(), Error> {
        let len = buf.remaining();
        self.0 = buf.copy_to_bytes(len);
        Ok(())
    }
}
impl BatchRecords for MemoryRecords {}

impl BatchRecords for RawRecords {}

/// size of the offset and length
pub const BATCH_PREAMBLE_SIZE: usize = size_of::<Offset>()     // Offset
        + size_of::<i32>(); // i32

pub const BATCH_FILE_HEADER_SIZE: usize = BATCH_PREAMBLE_SIZE + BATCH_HEADER_SIZE;

#[derive(Default, Debug)]
pub struct Batch<R = MemoryRecords> {
    pub base_offset: Offset,
    pub batch_len: i32, // only for decoding
    pub header: BatchHeader,
    records: R,
}

impl<R> Batch<R> {
    pub fn get_mut_header(&mut self) -> &mut BatchHeader {
        &mut self.header
    }

    pub fn get_header(&self) -> &BatchHeader {
        &self.header
    }

    #[inline(always)]
    pub fn own_records(self) -> R {
        self.records
    }

    #[inline(always)]
    pub fn records(&self) -> &R {
        &self.records
    }

    #[inline(always)]
    pub fn mut_records(&mut self) -> &mut R {
        &mut self.records
    }

    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    pub fn set_base_offset(&mut self, offset: Offset) {
        self.base_offset = offset;
    }

    pub fn base_offset(mut self, offset: Offset) -> Self {
        self.base_offset = offset;
        self
    }

    pub fn add_to_offset_delta(&mut self, delta: i32) {
        self.header.last_offset_delta += delta;
    }

    pub fn set_offset_delta(&mut self, delta: i32) {
        self.header.last_offset_delta = delta;
    }

    pub fn get_last_offset(&self) -> Offset {
        self.get_base_offset() + self.last_offset_delta() as Offset
    }

    pub fn records_len(&self) -> usize {
        if self.last_offset_delta() < 0 {
            return 0;
        }

        self.last_offset_delta() as usize + 1
    }
    /// get last offset delta
    #[deprecated(since = "0.9.2", note = "use last_offset_delta instead")]
    pub fn get_last_offset_delta(&self) -> Size {
        self.get_header().last_offset_delta as Size
    }

    pub fn last_offset_delta(&self) -> i32 {
        self.get_header().last_offset_delta
    }

    pub fn get_compression(&self) -> Result<Compression, CompressionError> {
        self.get_header().get_compression()
    }

    /// decode from buf stored in the file
    /// read all excluding records
    pub fn decode_from_file_buf<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("decoding preamble");
        self.base_offset.decode(src, version)?;
        self.batch_len.decode(src, version)?;
        self.header.decode(src, version)?;
        Ok(())
    }

    /// Return the size of the batch header + records
    pub fn batch_len(&self) -> i32 {
        self.batch_len
    }
}

impl TryFrom<Batch<RawRecords>> for Batch {
    type Error = CompressionError;
    fn try_from(batch: Batch<RawRecords>) -> Result<Self, Self::Error> {
        let records = batch.memory_records()?;
        Ok(Batch {
            base_offset: batch.base_offset,
            batch_len: (BATCH_HEADER_SIZE + records.write_size(0)) as i32,
            header: batch.header,
            records,
        })
    }
}

impl TryFrom<Batch> for Batch<RawRecords> {
    type Error = CompressionError;
    fn try_from(f: Batch) -> Result<Self, Self::Error> {
        let mut buf = Vec::new();
        f.records.encode(&mut buf, 0)?;

        let compression = f.get_compression()?;
        let compressed_records = compression.compress(&buf)?;
        let compressed_records_len = compressed_records.len() as i32;
        let records = RawRecords(compressed_records);

        Ok(Batch {
            base_offset: f.base_offset,
            batch_len: compressed_records_len,
            header: f.header,
            records,
        })
    }
}

impl<R> Batch<R>
where
    R: Encoder,
{
    /// check if batch is valid after decoded
    pub fn validate_decoding(&self) -> bool {
        self.batch_len == (BATCH_HEADER_SIZE + self.records.write_size(0)) as i32
    }
}

impl<R: BatchRecords> Batch<R> {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self::default()
    }

    /// create new batch with len
    pub fn new_with_len(len: i32) -> Self {
        Self {
            batch_len: len,
            ..Self::default()
        }
    }

    /// computed last offset which is base offset + number of records
    pub fn computed_last_offset(&self) -> Offset {
        self.get_base_offset() + self.records_len() as Offset
    }
}

impl Batch {
    /// add new record, this will update the offset to correct
    pub fn add_record(&mut self, record: Record) {
        self.add_records(&mut vec![record]);
        self.batch_len = (BATCH_HEADER_SIZE + self.records.write_size(0)) as i32;
    }

    pub fn add_records(&mut self, records: &mut Vec<Record>) {
        self.records.append(records);
        self.batch_len = (BATCH_HEADER_SIZE + self.records.write_size(0)) as i32;
        self.update_offset_deltas();
    }

    pub fn update_offset_deltas(&mut self) {
        for (index, record) in self.records.iter_mut().enumerate() {
            record.preamble.set_offset_delta(index as Offset);
        }
        self.header.last_offset_delta = self.records().len() as i32 - 1;
    }

    pub fn into_consumer_records_iter(
        self,
        partition: PartitionId,
    ) -> impl Iterator<Item = ConsumerRecord> {
        let base_offset = self.base_offset;
        let first_timestamp = self.header.first_timestamp;

        self.records
            .into_iter()
            .enumerate()
            .map(move |(relative, record)| ConsumerRecord {
                partition,
                offset: base_offset + relative as Offset,
                timestamp_base: first_timestamp,
                record,
            })
    }
}
impl Batch<RawRecords> {
    pub fn memory_records(&self) -> Result<MemoryRecords, CompressionError> {
        let compression = self.get_compression()?;

        let mut records: MemoryRecords = Default::default();
        if let Compression::None = compression {
            records.decode(&mut &self.records.0[..], 0)?;
        } else {
            let decompressed = compression
                .uncompress(&self.records.0[..])?
                .ok_or(CompressionError::UnreachableError)?;
            records.decode(&mut &decompressed[..], 0)?;
        }
        Ok(records)
    }
}

impl<T: Into<MemoryRecords>> From<T> for Batch {
    fn from(records: T) -> Self {
        let records = records.into();
        let mut batch = Self::default();

        let records: Vec<_> = records
            .into_iter()
            .enumerate()
            .map(|(i, mut record)| {
                record.preamble.set_offset_delta(i as Offset);
                record
            })
            .collect();

        batch.records = records;
        let len = batch.records.len() as i32;
        batch.batch_len = (BATCH_HEADER_SIZE + batch.records.write_size(0)) as i32;
        batch.header.last_offset_delta = if len > 0 { len - 1 } else { len };
        batch
    }
}

impl<R> Decoder for Batch<R>
where
    R: BatchRecords,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("decoding batch");
        self.decode_from_file_buf(src, version)?;

        let batch_len = self.batch_len as usize - BATCH_HEADER_SIZE;
        let mut buf = src.take(batch_len);
        if buf.remaining() < batch_len {
            return Err(Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "not enough buf records, expected: {}, found: {}",
                    batch_len,
                    buf.remaining()
                ),
            ));
        }

        self.records.decode(&mut buf, version)?;
        Ok(())
    }
}

// Record batch contains 12 bytes of pre-amble plus header + records
impl<R> Encoder for Batch<R>
where
    R: BatchRecords,
{
    fn write_size(&self, version: Version) -> usize {
        BATCH_FILE_HEADER_SIZE + self.records.write_size(version)
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        trace!("Encoding Batch");
        self.base_offset.encode(dest, version)?;
        let batch_len: i32 = (BATCH_HEADER_SIZE + self.records.write_size(version)) as i32;
        batch_len.encode(dest, version)?;

        // encode parts of header
        self.header.partition_leader_epoch.encode(dest, version)?;
        self.header.magic.encode(dest, version)?;

        let mut out: Vec<u8> = Vec::new();
        let buf = &mut out;
        self.header.attributes.encode(buf, version)?;
        self.header.last_offset_delta.encode(buf, version)?;
        self.header.first_timestamp.encode(buf, version)?;
        self.header.max_time_stamp.encode(buf, version)?;
        self.header.producer_id.encode(buf, version)?;
        self.header.producer_epoch.encode(buf, version)?;
        self.header.first_sequence.encode(buf, version)?;
        self.records.encode(buf, version)?;

        let crc = crc32c::crc32c(&out);
        crc.encode(dest, version)?;
        dest.put_slice(&out);
        Ok(())
    }
}

impl<R: Clone> Clone for Batch<R> {
    fn clone(&self) -> Self {
        Self {
            base_offset: self.base_offset,
            batch_len: self.batch_len,
            header: self.header.clone(),
            records: self.records.clone(),
        }
    }
}

#[derive(Debug, Decoder, Encoder, Clone)]
pub struct BatchHeader {
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    /// Indicates the count from the beginning of the batch to the end
    ///
    /// Adding this to the base_offset will give the offset of the last record in this batch
    pub last_offset_delta: i32,
    pub first_timestamp: Timestamp,
    pub max_time_stamp: Timestamp,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub first_sequence: i32,
}

impl BatchHeader {
    fn get_compression(&self) -> Result<Compression, CompressionError> {
        let compression_bits = self.attributes & COMPRESSION_CODEC_MASK;
        Compression::try_from(compression_bits as i8)
    }

    pub fn set_compression(&mut self, compression: Compression) {
        let compression_bits = compression as i16 & COMPRESSION_CODEC_MASK;
        self.attributes = (self.attributes & !COMPRESSION_CODEC_MASK) | compression_bits;
    }
}
impl Default for BatchHeader {
    fn default() -> Self {
        BatchHeader {
            partition_leader_epoch: -1,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta: -1,
            first_timestamp: NO_TIMESTAMP,
            max_time_stamp: NO_TIMESTAMP,
            producer_id: -1,
            producer_epoch: -1,
            first_sequence: -1,
        }
    }
}

pub const BATCH_HEADER_SIZE: usize = size_of::<i32>()     // partition leader epoch
        + size_of::<u8>()       // magic
        + size_of::<i32>()      //crc
        + size_of::<i16>()      // i16
        + size_of::<i32>()      // last offset delta
        + size_of::<i64>()      // first_timestamp
        + size_of::<i64>()      // max_time_stamp
        + size_of::<i64>()      //producer id
        + size_of::<i16>()      // produce_epoch
        + size_of::<i32>(); // first sequence

/// used for modifying timestamp as producer       
pub trait ProducerBatchHeader {
    fn set_first_timestamp(&mut self, timestamp: Timestamp);

    fn set_max_time_stamp(&mut self, timestamp: Timestamp);
}

impl ProducerBatchHeader for BatchHeader {
    fn set_first_timestamp(&mut self, timestamp: Timestamp) {
        self.first_timestamp = timestamp;
    }

    fn set_max_time_stamp(&mut self, timestamp: Timestamp) {
        self.max_time_stamp = timestamp;
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use std::io::Cursor;
    use std::io::Error as IoError;

    use crate::core::Decoder;
    use crate::core::Encoder;
    use crate::record::{Record, RecordData};
    use super::Batch;
    use super::BatchHeader;
    use super::BATCH_HEADER_SIZE;

    #[test]
    fn test_batch_convert_compression_size() {}

    #[test]
    fn test_batch_size() {
        let header = BatchHeader::default();
        assert_eq!(header.write_size(0), BATCH_HEADER_SIZE);
    }

    #[test]
    fn test_encode_and_decode_batch() -> Result<(), IoError> {
        let value = vec![0x74, 0x65, 0x73, 0x74];
        let record = Record {
            value: RecordData::from(value),
            ..Default::default()
        };
        let mut batch = Batch::<MemoryRecords>::default();
        batch.records.push(record);
        batch.header.first_timestamp = 1555478494747;
        batch.header.max_time_stamp = 1555478494747;

        let bytes = batch.as_bytes(0)?;
        println!("batch raw bytes: {:#X?}", bytes.as_ref());

        let batch = Batch::<MemoryRecords>::decode_from(&mut Cursor::new(bytes), 0)?;
        println!("batch: {batch:#?}");

        let decoded_record = batch.records.get(0).unwrap();
        println!("record crc: {}", batch.header.crc);
        assert_eq!(batch.header.crc, 1430948200);
        let b = decoded_record.value.as_ref();
        assert_eq!(b, b"test");
        assert!(batch.validate_decoding());

        Ok(())
    }

    /*  raw batch encoded

    0000   02 00 00 00 45 00 00 c7 00 00 40 00 40 06 00 00
    0010   c0 a8 07 30 c0 a8 07 30 d1 b9 23 84 29 ba 3d 48
    0020   0b 13 89 98 80 18 97 62 90 6a 00 00 01 01 08 0a
    0030   1e 6f 09 0d 1e 6f 09 06 00 00 00 8f 00 00 00 05
    0040   00 00 00 03 00 10 63 6f 6e 73 6f 6c 65 2d 70 72
    0050   6f 64 75 63 65 72 ff ff 00 01 00 00 05 dc 00 00
    0060   00 01 00 13 6d 79 2d 72 65 70 6c 69 63 61 74 65
    0070   64 2d 74 6f 70 69 63 00 00 00 01 00 00 00 00 00
    0080   00 00 48 00 00 00 00 00 00 00 00 00 00 00 3c ff
    0090   ff ff ff 02 5a 44 2c 31 00 00 00 00 00 00 00 00
    00a0   01 6a 29 be 3e 1b 00 00 01 6a 29 be 3e 1b ff ff
    00b0   ff ff ff ff ff ff ff ff ff ff ff ff 00 00 00 01
    00c0   14 00 00 00 01 08 74 65 73 74 00
    */

    #[test]
    fn test_batch_offset_delta() {
        let mut batch = Batch::<MemoryRecords>::default();
        assert_eq!(batch.get_base_offset(), 0);

        assert_eq!(batch.last_offset_delta(), -1);
        // last offset is -1 because there are no records in the batch
        assert_eq!(batch.get_last_offset(), -1);

        batch.add_record(Record::default());
        assert_eq!(batch.last_offset_delta(), 0);
        assert_eq!(batch.get_last_offset(), 0);

        batch.add_record(Record::default());
        assert_eq!(batch.last_offset_delta(), 1);
        assert_eq!(batch.get_last_offset(), 1);

        batch.add_record(Record::default());
        assert_eq!(batch.last_offset_delta(), 2);
        assert_eq!(batch.get_last_offset(), 2);

        assert_eq!(
            batch
                .records
                .get(0)
                .expect("index 0 should exists")
                .get_header()
                .get_offset_delta(),
            0
        );
        assert_eq!(
            batch
                .records
                .get(1)
                .expect("index 1 should exists")
                .get_header()
                .get_offset_delta(),
            1
        );
        assert_eq!(
            batch
                .records
                .get(2)
                .expect("index 2 should exists")
                .get_header()
                .get_offset_delta(),
            2
        );
    }

    #[test]
    fn test_batch_offset_diff_base() {
        let mut batch = Batch::<MemoryRecords>::default();
        batch.set_base_offset(1000);
        assert_eq!(batch.get_base_offset(), 1000);

        assert_eq!(batch.last_offset_delta(), -1);
        // last offset is -1 because there are no records in the batch
        assert_eq!(batch.get_last_offset(), 999);

        batch.add_record(Record::default());
        assert_eq!(batch.last_offset_delta(), 0);
        assert_eq!(batch.get_last_offset(), 1000);

        batch.add_record(Record::default());
        assert_eq!(batch.last_offset_delta(), 1);
        assert_eq!(batch.get_last_offset(), 1001);

        batch.add_record(Record::default());
        assert_eq!(batch.last_offset_delta(), 2);
        assert_eq!(batch.get_last_offset(), 1002);

        assert_eq!(
            batch
                .records
                .get(0)
                .expect("index 0 should exists")
                .get_header()
                .get_offset_delta(),
            0
        );
        assert_eq!(
            batch
                .records
                .get(1)
                .expect("index 1 should exists")
                .get_header()
                .get_offset_delta(),
            1
        );
        assert_eq!(
            batch
                .records
                .get(2)
                .expect("index 2 should exists")
                .get_header()
                .get_offset_delta(),
            2
        );
    }

    #[test]
    fn test_records_offset_delta() {
        let mut batch = Batch::<MemoryRecords>::default();
        batch.set_base_offset(2000);
        assert_eq!(batch.get_base_offset(), 2000);

        // add records directly
        batch.records.append(&mut vec![
            Record::default(),
            Record::default(),
            Record::default(),
        ]);
        batch.update_offset_deltas();
        assert_eq!(batch.last_offset_delta(), 2);
        assert_eq!(batch.get_last_offset(), 2002);

        assert_eq!(
            batch
                .records
                .get(0)
                .expect("index 0 should exists")
                .get_header()
                .get_offset_delta(),
            0
        );
        assert_eq!(
            batch
                .records
                .get(1)
                .expect("index 1 should exists")
                .get_header()
                .get_offset_delta(),
            1
        );
        assert_eq!(
            batch
                .records
                .get(2)
                .expect("index 2 should exists")
                .get_header()
                .get_offset_delta(),
            2
        );
    }

    #[test]
    fn test_batch_records_offset() {
        let mut comparison = Batch::<MemoryRecords>::default();
        comparison.add_record(Record::default());
        comparison.add_record(Record::default());
        comparison.add_record(Record::default());

        let batch_created = Batch::from(vec![
            Record::default(),
            Record::default(),
            Record::default(),
        ]);

        for i in 0..3 {
            assert_eq!(
                batch_created
                    .records
                    .get(i)
                    .expect("get record")
                    .get_header()
                    .get_offset_delta(),
                comparison
                    .records
                    .get(i)
                    .expect("get record")
                    .get_header()
                    .get_offset_delta(),
                "Creating a Batch from a Vec gave wrong delta",
            )
        }

        assert_eq!(batch_created.last_offset_delta(), 2);
    }

    #[test]
    fn test_into_consumer_records_iter() {
        let mut batch = Batch::from(vec![
            Record::default(),
            Record::default(),
            Record::default(),
        ]);

        batch.header.first_timestamp = 1_500_000_000;
        let partition_id = 1;

        let consumer_records = batch
            .into_consumer_records_iter(partition_id)
            .collect::<Vec<ConsumerRecord>>();
        assert_eq!(consumer_records.len(), 3);
        assert_eq!(consumer_records[0].offset(), 0);
        assert_eq!(consumer_records[1].offset(), 1);
        assert_eq!(consumer_records[2].offset(), 2);

        consumer_records.iter().for_each(|record| {
            assert_eq!(record.timestamp(), 1_500_000_000);
            assert_eq!(record.partition, partition_id);
        });
    }

    #[test]
    fn test_batch_len() {
        let mem_records = vec![Record::default(), Record::default(), Record::default()];

        // Verify batch len is instantiated
        let batch = Batch::from(mem_records.clone());

        assert_eq!(
            batch.batch_len(),
            (BATCH_HEADER_SIZE + mem_records.write_size(0)) as i32
        );

        // Verify batch len is preserved during conversion
        let batch_raw_records: Batch<RawRecords> = Batch::try_from(batch).unwrap();
        assert_eq!(
            batch_raw_records.batch_len(),
            mem_records.write_size(0) as i32
        );

        // Verify batch len is preserved during conversion
        let batch: Batch = batch_raw_records.try_into().unwrap();
        assert_eq!(
            batch.batch_len(),
            (BATCH_HEADER_SIZE + mem_records.write_size(0)) as i32
        );

        // Verify increase in batch len when we add records
        let mut batch_mem_records: Batch<MemoryRecords> = Batch::new();
        batch_mem_records.add_records(&mut mem_records.clone());
        assert_eq!(
            batch_mem_records.batch_len(),
            (BATCH_HEADER_SIZE + mem_records.write_size(0)) as i32
        );

        batch_mem_records.add_record(Record::default());
        assert_eq!(
            batch_mem_records.batch_len(),
            (BATCH_HEADER_SIZE + batch_mem_records.records.write_size(0)) as i32
        );

        // Test compressed batch is smaller than non compressed when converted to Batch<RawRecords>
        // using record with easy to compress data
        let test_record = Record::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let mut test_records = vec![test_record.clone(), test_record.clone(), test_record];

        let mut batch_mem_1: Batch = Batch::new();
        batch_mem_1.add_records(&mut test_records.clone());

        let mut batch_mem_2: Batch = Batch::new();
        batch_mem_2.add_records(&mut test_records);

        // Verify we're starting from the same length
        assert_eq!(batch_mem_1.batch_len(), batch_mem_2.batch_len());

        // Apply compression and compare resulting sizes
        let no_compression = Compression::None;
        let compression = Compression::Gzip;

        let header_1 = batch_mem_1.get_mut_header();
        let header_2 = batch_mem_2.get_mut_header();

        header_1.set_compression(no_compression);
        header_2.set_compression(compression);

        let not_compressed: Batch<RawRecords> = Batch::try_from(batch_mem_1).unwrap();
        let compressed: Batch<RawRecords> = Batch::try_from(batch_mem_2).unwrap();

        assert_ne!(not_compressed.batch_len(), compressed.batch_len());
        assert!(not_compressed.batch_len() > compressed.batch_len());
    }
}
