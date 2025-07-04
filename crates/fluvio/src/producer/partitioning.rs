use std::sync::atomic::{AtomicU32, Ordering};
use siphasher::sip::SipHasher;
use fluvio_types::{PartitionId, PartitionCount};

/// A trait for defining a partitioning strategy for key/value records.
///
/// A Partitioner is given a slice of potential keys, and the number of
/// partitions in the current Topic. It must map each key from the input
/// slice into a partition stored at the same index in the output Vec.
///
/// It is up to the implementor to decide how the keys get mapped to
/// partitions. This includes deciding what partition to assign to records
/// with no keys (represented by `None` values in the keys slice).
///
/// See [`SiphashRoundRobinPartitioner`] for a reference implementation.
pub trait Partitioner {
    fn partition(
        &self,
        config: &PartitionerConfig,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> PartitionId;
}

pub struct PartitionerConfig {
    pub partition_count: PartitionCount,
    pub available_partitions: Vec<PartitionCount>,
}

impl PartitionerConfig {
    /// Creates a new `PartitionerConfig` with the given partition count and available partitions.
    pub fn partition_count(&self) -> PartitionCount {
        self.partition_count
    }
}

/// A [`Partitioner`] which combines hashing and round-robin partition assignment
///
/// - Records with keys get their keys hashed with siphash
/// - Records without keys get assigned to partitions using round-robin
pub(crate) struct SiphashRoundRobinPartitioner {
    index: AtomicU32,
}

impl SiphashRoundRobinPartitioner {
    pub fn new() -> Self {
        Self {
            index: AtomicU32::new(0),
        }
    }
}

impl Partitioner for SiphashRoundRobinPartitioner {
    fn partition(
        &self,
        config: &PartitionerConfig,
        maybe_key: Option<&[u8]>,
        _value: &[u8],
    ) -> PartitionId {
        match maybe_key {
            Some(key) => partition_siphash(key, config.partition_count()),
            None => {
                // Atomic increment. This will wrap on overflow, which is fine
                // because we are only interested in the modulus anyway
                let index = self.index.fetch_add(1, Ordering::Relaxed);
                if config.available_partitions.is_empty() {
                    return index % config.partition_count;
                }
                let partition = index as usize % config.available_partitions.len();
                config.available_partitions[partition]
            }
        }
    }
}

fn partition_siphash(key: &[u8], partition_count: PartitionCount) -> PartitionId {
    use std::hash::{Hash, Hasher};

    let mut hasher = SipHasher::new();
    key.hash(&mut hasher);
    let hashed = hasher.finish();

    let partition_id = hashed % partition_count as u64;
    match PartitionId::try_from(partition_id) {
        Ok(partition_id) => partition_id,
        Err(_) => panic!("partition_siphash failed for partition_count={partition_count} "),
    }
}

/// A [`Partitioner`] which assigns all records to a specific partition
pub(crate) struct SpecificPartitioner {
    partition_id: PartitionId,
}

impl SpecificPartitioner {
    pub fn new(partition_id: PartitionId) -> Self {
        Self { partition_id }
    }
}

impl Partitioner for SpecificPartitioner {
    fn partition(
        &self,
        _config: &PartitionerConfig,
        _maybe_key: Option<&[u8]>,
        _value: &[u8],
    ) -> PartitionId {
        self.partition_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ensure that feeding keyless records one-at-a-time does not assign the same partition
    #[test]
    fn test_round_robin_individual() {
        let config = PartitionerConfig {
            partition_count: 3,
            available_partitions: vec![0, 1, 2],
        };
        let partitioner = SiphashRoundRobinPartitioner::new();

        let key1_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key1_partition, 0);
        let key2_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key2_partition, 1);
        let key3_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key3_partition, 2);
        let key4_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key4_partition, 0);
        let key5_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key5_partition, 1);
        let key6_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key6_partition, 2);
    }

    #[test]
    fn test_parallel_partitioning() {
        use std::sync::Arc;

        let (tx, rx) = std::sync::mpsc::channel();
        let partitioner = Arc::new(SiphashRoundRobinPartitioner::new());
        let config = Arc::new(PartitionerConfig {
            partition_count: 4,
            available_partitions: vec![0, 1, 2, 3],
        });

        // We have 5 threads calculating partitions 400 times each for NULL key (aka round-robin).
        // This is 20,000 records total, among 4 partitions. If it is evenly distributed like we
        // want (and the atomic counter is working as expected), we should get exactly 500
        // hits for each partition.
        for _ in 0..5 {
            let tx = tx.clone();
            let partitioner = partitioner.clone();
            let config = config.clone();
            std::thread::spawn(move || {
                for _ in 0..400 {
                    let partition = partitioner.partition(&config, None, &[]);
                    tx.send(partition).unwrap();
                }
            });
        }
        drop(tx);

        let mut counts = std::collections::HashMap::new();
        while let Ok(partition) = rx.recv() {
            let partition_count = counts.entry(partition).or_insert(0);
            *partition_count += 1;
        }

        for (_partition, &count) in counts.iter() {
            assert_eq!(count, 500);
        }
    }

    #[test]
    fn test_available_partitions() {
        let config = PartitionerConfig {
            partition_count: 3,
            available_partitions: vec![0, 2], // Only partitions 0 and 2 are available
        };
        let partitioner = SiphashRoundRobinPartitioner::new();

        let key1_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key1_partition, 0);
        let key2_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key2_partition, 2);
        let key3_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key3_partition, 0);
        let key4_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key4_partition, 2);
        let key5_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key5_partition, 0);
        let key6_partition = partitioner.partition(&config, None, &[]);
        assert_eq!(key6_partition, 2);
    }
}
