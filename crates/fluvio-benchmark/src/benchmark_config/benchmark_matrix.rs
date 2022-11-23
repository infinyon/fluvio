use std::time::Duration;
use std::fs::File;
use serde::{Deserialize, Serialize};
use fluvio::Compression;
use super::benchmark_settings::{BenchmarkSettings, BenchmarkBuilder, CrossIterate};

/// Key used by AllShareSameKey
pub const SHARED_KEY: &str = "SHARED_KEY";

/// DEFAULT CONFIG DIR
pub const DEFAULT_CONFIG_DIR: &str = "crates/fluvio-benchmark/benches";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SharedSettings {
    pub matrix_name: String,
    pub num_samples: usize,
    pub millis_between_samples: u64,
    pub worker_timeout_seconds: u64,
}

/// A BenchmarkMatrix contains a collection of settings and dimensions.
/// Iterating over a BenchmarkMatrix produces a BenchmarkSettings for every possible combination of values in the matrix.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BenchmarkMatrix {
    pub shared_settings: SharedSettings,

    // Dimensions
    pub num_records_per_producer_worker_per_batch: Vec<u64>,
    pub producer_batch_size: Vec<u64>,
    pub producer_queue_size: Vec<u64>,
    pub producer_linger_millis: Vec<u64>,
    pub producer_server_timeout_millis: Vec<u64>,
    pub producer_compression: Vec<Compression>,
    pub record_key_allocation_strategy: Vec<RecordKeyAllocationStrategy>,
    // TODO
    // pub producer_isolation:...,
    // TODO
    // pub producer_delivery_semantic,
    pub consumer_max_bytes: Vec<u64>,
    // TODO
    // pub consumer_isolation:...,
    pub num_concurrent_producer_workers: Vec<u64>,
    /// Total number of concurrent consumers equals num_concurrent_consumers_per_partition * num_partitions
    pub num_concurrent_consumers_per_partition: Vec<u64>,
    pub num_partitions: Vec<u64>,
    // TODO
    // pub num_replicas: Vec<u64>,
    pub record_size: Vec<u64>,
    // TODO
    // pub use_smart_module: Vec<bool>,
    // TODO
    // IgnoreRac
}

impl IntoIterator for BenchmarkMatrix {
    type Item = BenchmarkSettings;

    type IntoIter = <Vec<BenchmarkSettings> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.generate_settings().into_iter()
    }
}

impl BenchmarkMatrix {
    // Impl note: This does allocate for all of the benchmark settings at once, however it made for simpler code
    // and as there is a very low practical limit for the number of benchmarks that can be run in a reasonable time period, its not an issue that it alloates.
    // TODO split into smaller chunks

    fn generate_settings(&self) -> Vec<BenchmarkSettings> {
        let builder = vec![BenchmarkBuilder::new(&self.shared_settings)];
        builder
            .cross_iterate(&self.num_records_per_producer_worker_per_batch, |v, b| {
                b.num_records_per_producer_worker_per_batch = Some(v);
            })
            .cross_iterate(&self.producer_batch_size, |v, b| {
                b.producer_batch_size = Some(v);
            })
            .cross_iterate(&self.producer_queue_size, |v, b| {
                b.producer_queue_size = Some(v);
            })
            .cross_iterate(&self.producer_linger_millis, |v, b| {
                b.producer_linger = Some(Duration::from_millis(v));
            })
            .cross_iterate(&self.producer_server_timeout_millis, |v, b| {
                b.producer_server_timeout = Some(Duration::from_millis(v));
            })
            .cross_iterate(&self.producer_compression, |v, b| {
                b.producer_compression = Some(v);
            })
            .cross_iterate(&self.consumer_max_bytes, |v, b| {
                b.consumer_max_bytes = Some(v);
            })
            .cross_iterate(&self.num_concurrent_producer_workers, |v, b| {
                b.num_concurrent_producer_workers = Some(v);
            })
            .cross_iterate(&self.num_concurrent_consumers_per_partition, |v, b| {
                b.num_concurrent_consumers_per_partition = Some(v);
            })
            .cross_iterate(&self.num_partitions, |v, b| {
                b.num_partitions = Some(v);
            })
            .cross_iterate(&self.record_size, |v, b| {
                b.record_size_strategy = Some(v);
            })
            .cross_iterate(&self.record_key_allocation_strategy, |v, b| {
                b.record_key_allocation_strategy = Some(v);
            })
            .build()
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Hash, PartialEq, Eq)]
pub enum RecordKeyAllocationStrategy {
    /// RecordKey::NULL
    NoKey,
    /// All producer workers will use the same key
    AllShareSameKey,

    /// Each producer will use the same key for each of their records
    ProducerWorkerUniqueKey,

    /// Each producer will round robin from 0..N for each record produced
    RoundRobinKey(u64),

    RandomKey,
}

pub fn get_default_config() -> Vec<BenchmarkMatrix> {
    walkdir::WalkDir::new(DEFAULT_CONFIG_DIR)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            if e.path().extension().is_some() {
                Some(e)
            } else {
                None
            }
        })
        .map(|e| {
            let file = File::open(e.path()).unwrap();
            serde_yaml::from_reader::<_, BenchmarkMatrix>(file).unwrap()
        })
        .collect()
}

pub fn get_config_from_file(path: &str) -> Vec<BenchmarkMatrix> {
    let file = File::open(path).unwrap();
    vec![serde_yaml::from_reader::<_, BenchmarkMatrix>(file).unwrap()]
}

#[cfg(test)]
mod tests {

    use std::{path::Path, fs::File};

    use super::BenchmarkMatrix;

    fn test_config(path: &Path) -> bool {
        let file = File::open(path).expect("Unable to open file");
        match serde_yaml::from_reader::<_, BenchmarkMatrix>(file) {
            Ok(_) => true,
            Err(e) => {
                println!("failed to parse configuration at {}: {}", path.display(), e);
                false
            }
        }
    }

    #[test]
    fn load_configs() {
        // make sure all the configs in benches can be parsed correctly
        let mut success = true;
        for file in walkdir::WalkDir::new("benches")
            .into_iter()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                if e.path().extension().is_some() {
                    Some(e)
                } else {
                    None
                }
            })
        {
            success = success && test_config(file.path());
        }
        assert!(success, "one or more configuration files failed to parse");
    }
}
