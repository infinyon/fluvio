use std::{fs::File, sync::Arc};
use std::io;
use std::io::Read;
use std::path::Path;

use tracing::info;
use derive_builder::Builder;

use crate::record::{DefaultRecord, RecordSet};
use crate::batch::DefaultBatch;

const DEFAULT_TEST_BYTE: u8 = 5;

fn default_record_producer(_record_index: usize, producer: &BatchProducer) -> DefaultRecord {
    let mut record = DefaultRecord::default();
    let bytes: Vec<u8> = vec![DEFAULT_TEST_BYTE; producer.per_record_bytes];
    record.value = bytes.into();
    record
}

#[derive(Builder)]
pub struct BatchProducer {
    #[builder(setter(into), default = "0")]
    producer_id: i64,
    #[builder(setter(into), default = "1")]
    pub records: u16,
    /// how many bytes in a record
    #[builder(setter, default = "2")]
    pub per_record_bytes: usize,
    /// generate record
    #[builder(setter, default = "Arc::new(default_record_producer)")]
    pub record_generator: Arc<dyn Fn(usize, &BatchProducer) -> DefaultRecord>,
}

impl BatchProducer {
    pub fn builder() -> BatchProducerBuilder {
        BatchProducerBuilder::default()
    }

    fn generate_batch(&self) -> DefaultBatch {
        let mut batches = DefaultBatch::default();
        let header = batches.get_mut_header();
        header.magic = 2;
        header.producer_id = self.producer_id;
        header.producer_epoch = -1;

        for i in 0..self.records {
            batches.add_record((self.record_generator)(i as usize, &self));
        }

        batches
    }

    pub fn records(&self) -> RecordSet {
        RecordSet::default().add(self.generate_batch())
    }
}

pub fn create_batch() -> DefaultBatch {
    create_batch_with_producer(12, 2)
}

/// create batches with produce and records count
pub fn create_batch_with_producer(producer: i64, records: u16) -> DefaultBatch {
    let mut batches = DefaultBatch::default();
    let header = batches.get_mut_header();
    header.magic = 2;
    header.producer_id = producer;
    header.producer_epoch = -1;

    for _ in 0..records {
        let mut record = DefaultRecord::default();
        let bytes: Vec<u8> = vec![10, 20];
        record.value = bytes.into();
        batches.add_record(record);
    }

    batches
}

pub fn read_bytes_from_file<P>(path: P) -> Result<Vec<u8>, io::Error>
where
    P: AsRef<Path>,
{
    let file_path = path.as_ref();
    info!("test file: {}", file_path.display());
    let mut f = File::open(file_path)?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;
    Ok(buffer)
}
