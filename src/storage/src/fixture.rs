use tracing::info;
use std::{fs::File, sync::Arc};
use std::io;
use std::io::Read;
use std::path::Path;
use std::env::temp_dir;

use derive_builder::Builder;

use dataplane::record::{DefaultRecord, RecordSet};
use dataplane::batch::DefaultBatch;
use dataplane::Size;

use crate::config::ConfigOption;

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

pub fn default_option(index_max_interval_bytes: Size) -> ConfigOption {
    ConfigOption {
        segment_max_bytes: 100,
        index_max_interval_bytes,
        base_dir: temp_dir(),
        index_max_bytes: 1000,
        ..Default::default()
    }
}

#[cfg(test)]
mod pin_tests {

    use std::pin::Pin;
    use pin_utils::pin_mut;
    use pin_utils::unsafe_unpinned;

    //  impl Unpin for Counter{}

    struct Counter {
        total: u16,
    }

    impl Counter {
        unsafe_unpinned!(total: u16);

        fn get_total(self: Pin<&mut Self>) -> u16 {
            self.total
        }

        fn update_total(mut self: Pin<&mut Self>, val: u16) {
            *self.as_mut().total() = val;
        }
    }

    #[test]
    fn test_read_pin() {
        let counter = Counter { total: 20 };
        pin_mut!(counter); // works with future that requires unpin
        assert_eq!(counter.get_total(), 20);
    }

    #[test]
    fn test_write_pin() {
        let counter = Counter { total: 20 };
        pin_mut!(counter); // works with future that requires unpin
        counter.as_mut().update_total(30);
        assert_eq!(counter.get_total(), 30);
    }
}
