use std::{env::temp_dir, sync::Arc};

use derive_builder::Builder;

use fluvio_protocol::record::{Size, Batch, Record};

use crate::{
    config::{ReplicaConfig, StorageConfig},
    StorageError,
};

pub fn default_option(index_max_interval_bytes: Size) -> ReplicaConfig {
    ReplicaConfig {
        segment_max_bytes: 100,
        index_max_interval_bytes,
        base_dir: temp_dir(),
        index_max_bytes: 1000,
        ..Default::default()
    }
}

#[derive(Builder)]
pub struct BatchProducer {
    #[builder(setter(into), default = "0")]
    base_offset: i64,
    #[builder(setter(into), default = "0")]
    producer_id: i64,
    #[builder(setter(into), default = "2")]
    pub records: u16,
    /// how many bytes in a record
    #[builder(setter, default = "2")]
    pub per_record_bytes: usize,
}

impl BatchProducer {
    pub fn builder() -> BatchProducerBuilder {
        BatchProducerBuilder::default()
    }

    // create new batch
    pub fn batch(&mut self) -> Batch {
        self.batch_records(self.records)
    }

    // create new batch
    pub fn batch_records(&mut self, records: u16) -> Batch {
        let mut batch = Batch::default();
        batch.set_base_offset(self.base_offset);
        let header = batch.get_mut_header();
        header.magic = 2;
        header.producer_id = self.producer_id;
        header.producer_epoch = -1;
        for _ in 0..records {
            let record = Record::new(vec![10, 20]);
            batch.add_record(record);
        }
        self.base_offset += records as i64;
        batch
    }
}

pub fn storage_config() -> Arc<StorageConfig> {
    let clear_config = StorageConfig::builder()
        .build()
        .map_err(|err| StorageError::Other(format!("failed to build cleaner config: {err}")))
        .expect("config");
    Arc::new(clear_config)
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
