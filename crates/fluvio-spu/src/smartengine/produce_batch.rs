use std::io::{Error, ErrorKind};
use fluvio_protocol::record::{Batch, RawRecords};

pub struct ProduceBatch<'a> {
    pub(crate) batch: &'a Batch<RawRecords>,
    pub(crate) records: Vec<u8>,
}

pub struct ProduceBatchIterator<'a> {
    batches: &'a Vec<Batch<RawRecords>>,
    index: usize,
    len: usize,
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
    type Item = Result<ProduceBatch<'a>, Error>;

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
                return Some(Err(Error::new(
                    ErrorKind::Other,
                    format!("unknown compression value for batch {err}"),
                )))
            }
        };

        let records = match compression.uncompress(raw_bytes) {
            Ok(Some(records)) => records,
            Ok(None) => raw_bytes.to_vec(),
            Err(err) => {
                return Some(Err(Error::new(
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
