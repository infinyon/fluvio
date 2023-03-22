use std::io::{Error as IoError, ErrorKind};
use fluvio_protocol::record::{Batch, RawRecords, Offset};

use super::batch::SmartModuleInputBatch;
use fluvio_compression::{Compression, CompressionError};

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
