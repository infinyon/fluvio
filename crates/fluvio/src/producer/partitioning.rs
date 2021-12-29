use std::sync::atomic::{AtomicI32, Ordering};
use siphasher::sip::SipHasher;
use fluvio_types::PartitionId;

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
    pub(crate) partition_count: i32,
}

/// A [`Partitioner`] which combines hashing and round-robin partition assignment
///
/// - Records with keys get their keys hashed with siphash
/// - Records without keys get assigned to partitions using round-robin
pub(crate) struct SiphashRoundRobinPartitioner {
    index: AtomicI32,
}

impl SiphashRoundRobinPartitioner {
    pub fn new() -> Self {
        Self {
            index: AtomicI32::new(0),
        }
    }
}

impl Partitioner for SiphashRoundRobinPartitioner {
    fn partition(
        &self,
        config: &PartitionerConfig,
        maybe_key: Option<&[u8]>,
        _value: &[u8],
    ) -> i32 {
        match maybe_key {
            Some(key) => partition_siphash(key, config.partition_count),
            None => {
                // Atomic increment. This will wrap on overflow, which is fine
                // because we are only interested in the modulus anyway
                let partition = self.index.fetch_add(1, Ordering::Relaxed);
                partition % config.partition_count
            }
        }
    }
}

fn partition_siphash(key: &[u8], partition_count: i32) -> i32 {
    use std::hash::{Hash, Hasher};

    assert!(partition_count >= 0, "Partition must not be less than zero");
    let mut hasher = SipHasher::new();
    key.hash(&mut hasher);
    let hashed = hasher.finish();

    i32::try_from(hashed % partition_count as u64).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ensure that feeding keyless records one-at-a-time does not assign the same partition
    #[test]
    fn test_round_robin_individual() {
        let config = PartitionerConfig { partition_count: 3 };
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
        let config = Arc::new(PartitionerConfig { partition_count: 4 });

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
}
