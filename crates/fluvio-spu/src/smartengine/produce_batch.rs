use std::io::{Error as IoError, ErrorKind};
use fluvio_protocol::record::{Batch, RawRecords, Offset};

use super::batch::SmartModuleInputBatch;
use fluvio_compression::{Compression, CompressionError};

#[derive(Debug)]
pub struct ProduceBatch<'a> {
    pub(crate) batch: &'a Batch<RawRecords>,
    pub(crate) records: Vec<u8>,
}

pub struct ProduceBatchIterator<'a> {
    batches: &'a Vec<Batch<RawRecords>>,
    index: usize,
    len: usize,
}

impl<'a> SmartModuleInputBatch for ProduceBatch<'a> {
    fn records(&self) -> &Vec<u8> {
        &self.records
    }

    fn base_offset(&self) -> Offset {
        self.batch.base_offset
    }

    fn offset_delta(&self) -> i32 {
        self.batch.header.last_offset_delta
    }

    fn get_compression(&self) -> Result<Compression, CompressionError> {
        self.batch.get_compression()
    }
}

impl<'a> ProduceBatchIterator<'a> {
    pub fn new(batches: &'a Vec<Batch<RawRecords>>) -> Self {
        Self {
            batches,
            index: 0,
            len: batches.len(),
        }
    }
}

impl<'a> Iterator for ProduceBatchIterator<'a> {
    type Item = Result<ProduceBatch<'a>, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.len {
            return None;
        }

        let batch = &self.batches[self.index];

        let r = batch.records();
        let raw_bytes = &r.0;

        let compression = match batch.get_compression() {
            Ok(compression) => compression,
            Err(err) => {
                return Some(Err(IoError::new(
                    ErrorKind::Other,
                    format!("unknown compression value for batch {err}"),
                )))
            }
        };

        let records = match compression.uncompress(raw_bytes) {
            Ok(Some(records)) => records,
            Ok(None) => raw_bytes.to_vec(),
            Err(err) => {
                return Some(Err(IoError::new(
                    ErrorKind::Other,
                    format!("uncompress error {err}"),
                )))
            }
        };

        let produce_batch = ProduceBatch { batch, records };

        self.index += 1;

        Some(Ok(produce_batch))
    }
}

#[cfg(test)]
mod test {
    use fluvio_protocol::record::{Batch, RawRecords};
    use super::ProduceBatchIterator;
    use bytes::Bytes;
    use fluvio::dataplane::record::Record;
    use fluvio_compression::Compression;

    #[test]
    fn test_produce_batch_iterator() {
        let value1: Bytes = Bytes::from_static(b"soup");
        let value2: Bytes = Bytes::from_static(b"fries");

        let mut batch1 = Batch::default();
        batch1.header.set_compression(Compression::Gzip);
        batch1.add_record(Record::new(value1));

        let mut batch2 = Batch::default();
        batch2.header.set_compression(Compression::Lz4);
        batch2.add_record(Record::new(value2));

        let batches = vec![
            Batch::<RawRecords>::try_from(batch1).unwrap(),
            Batch::<RawRecords>::try_from(batch2).unwrap(),
        ];

        let mut produce_batch_iterator = ProduceBatchIterator::new(&batches);

        assert_eq!(
            produce_batch_iterator.next().unwrap().unwrap().records,
            b"\0\0\0\x01\x14\0\0\0\0\x08soup\0"
        );
        assert_eq!(
            produce_batch_iterator.next().unwrap().unwrap().records,
            b"\0\0\0\x01\x16\0\0\0\0\nfries\0"
        );
    }
}
