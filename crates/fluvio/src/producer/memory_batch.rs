use chrono::Utc;

use fluvio_protocol::{
    record::{RawRecords, Batch, Offset, MemoryRecords, BATCH_HEADER_SIZE, ProducerBatchHeader},
    Encoder,
};
use fluvio_types::Timestamp;

use super::*;

pub struct MemoryBatch {
    compression: Compression,
    write_limit: usize,
    current_size_uncompressed: usize,
    is_full: bool,
    create_time: Timestamp,
    records: Vec<Record>,
}
impl MemoryBatch {
    pub fn new(write_limit: usize, compression: Compression) -> Self {
        let now = Utc::now().timestamp_millis();
        Self {
            compression,
            is_full: false,
            write_limit,
            create_time: now,
            current_size_uncompressed: Vec::<RawRecords>::default().write_size(0),
            records: vec![],
        }
    }

    pub(crate) fn compression(&self) -> Compression {
        self.compression
    }

    /// Add a record to the batch.
    /// The value of `Offset` is relative to the `MemoryBatch` instance.
    pub fn push_record(&mut self, mut record: Record) -> Option<Offset> {
        let current_offset = self.offset() as i64;
        record
            .get_mut_header()
            .set_offset_delta(current_offset as Offset);

        let timestamp_delta = self.elapsed();
        record.get_mut_header().set_timestamp_delta(timestamp_delta);

        let record_size = record.write_size(0);

        if self.estimated_size() + record_size > self.write_limit {
            self.is_full = true;
            return None;
        }

        if self.estimated_size() + record_size == self.write_limit {
            self.is_full = true;
        }

        self.current_size_uncompressed += record_size;

        self.records.push(record);

        Some(current_offset)
    }

    pub fn is_full(&self) -> bool {
        self.is_full || self.write_limit <= self.estimated_size()
    }

    pub fn elapsed(&self) -> Timestamp {
        let now = Utc::now().timestamp_millis();

        std::cmp::max(0, now - self.create_time)
    }

    fn estimated_size(&self) -> usize {
        (self.current_size_uncompressed as f32
            * match self.compression {
                Compression::None => 1.0,
                Compression::Gzip | Compression::Snappy | Compression::Lz4 => 0.5,
            }) as usize
            + Batch::<RawRecords>::default().write_size(0)
    }

    pub fn records_len(&self) -> usize {
        self.records.len()
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.records_len()
    }

    pub fn current_size_uncompressed(&self) -> usize {
        self.current_size_uncompressed
    }
}

impl From<MemoryBatch> for Batch<MemoryRecords> {
    fn from(p_batch: MemoryBatch) -> Self {
        let mut batch =
            Self::new_with_len((BATCH_HEADER_SIZE + p_batch.records.write_size(0)) as i32);

        let compression = p_batch.compression();
        let records = p_batch.records;

        let len = records.len() as i32;
        batch.set_base_offset(if len > 0 { len - 1 } else { len } as i64);

        let header = batch.get_mut_header();
        header.last_offset_delta = if len > 0 { len - 1 } else { len };

        let first_timestamp = p_batch.create_time;

        let max_time_stamp = records
            .last()
            .map(|r| first_timestamp + r.timestamp_delta())
            .unwrap_or(0);

        header.set_first_timestamp(first_timestamp);
        header.set_max_time_stamp(max_time_stamp);

        header.set_compression(compression);

        *batch.mut_records() = records;

        batch
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_memory_batch() {
        use super::MemoryBatch;

        let record = Record::from(("key", "value"));
        let size = record.write_size(0);

        let mut mb = MemoryBatch::new(
            size * 4
                + Batch::<RawRecords>::default().write_size(0)
                + Vec::<RawRecords>::default().write_size(0),
            Compression::None,
        );

        assert!(mb.push_record(record).is_some());
        std::thread::sleep(std::time::Duration::from_millis(100));
        let record = Record::from(("key", "value"));
        assert!(mb.push_record(record).is_some());
        std::thread::sleep(std::time::Duration::from_millis(100));
        let record = Record::from(("key", "value"));
        assert!(mb.push_record(record).is_some());

        let batch: Batch<MemoryRecords> = mb.try_into().expect("failed to convert");
        assert!(
            batch.header.first_timestamp > 0,
            "first_timestamp is {}",
            batch.header.first_timestamp
        );
        assert!(
            batch.header.first_timestamp < batch.header.max_time_stamp,
            "first_timestamp: {}, max_time_stamp: {}",
            batch.header.first_timestamp,
            batch.header.max_time_stamp
        );

        let records_delta: Vec<_> = batch
            .records()
            .iter()
            .map(|record| record.timestamp_delta())
            .collect();
        assert_eq!(records_delta[0], 0);
        assert!(
            (100..150).contains(&records_delta[1]),
            "records_delta[1]: {}",
            records_delta[1]
        );
        assert!(
            (200..250).contains(&records_delta[2]),
            "records_delta[2]: {}",
            records_delta[2]
        );
    }

    #[test]
    fn test_convert_memory_batch_to_batch() {
        let num_records = 10;

        let record_data = "I am test input".to_string().into_bytes();
        let memory_batch_compression = Compression::Gzip;

        // This MemoryBatch write limit is minimal value to pass test
        let mut memory_batch = MemoryBatch::new(180, memory_batch_compression);

        let mut offset = 0;

        for _ in 0..num_records {
            offset = memory_batch
                .push_record(Record {
                    value: RecordData::from(record_data.clone()),
                    ..Default::default()
                })
                .expect("Offset should exist");
        }

        let memory_batch_records_len = memory_batch.records_len();
        let memory_batch_size_uncompressed = memory_batch.current_size_uncompressed();

        let batch: Batch<MemoryRecords> = memory_batch.into();

        assert_eq!(
            batch.get_base_offset(),
            (memory_batch_records_len as i64) - 1
        );

        assert_eq!(batch.last_offset_delta(), offset as i32);
        assert_eq!(batch.get_base_offset() as i32, batch.last_offset_delta());

        assert_eq!(
            batch.get_compression().expect("Compression should exist"),
            memory_batch_compression
        );

        assert_eq!(batch.records_len(), memory_batch_records_len);

        assert_eq!(
            batch.batch_len(),
            (BATCH_HEADER_SIZE + memory_batch_size_uncompressed) as i32
        );
    }
}
