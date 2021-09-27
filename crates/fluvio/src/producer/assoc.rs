use std::collections::{HashSet, HashMap};
use dataplane::{ReplicaKey, Offset, ErrorCode};
use dataplane::batch::Batch;
use dataplane::record::Record;
use dataplane::produce::{ProduceRequest, TopicProduceData, PartitionProduceData, ProduceResponse};
use crate::producer::{RecordUid, ProducerError};

use crate::producer::Result;

/// Represents a Record sitting in the batch queue, waiting to be sent.
#[derive(Debug)]
pub(crate) struct AssociatedRecord {
    pub uid: RecordUid,
    pub replica_key: ReplicaKey,
    pub record: Record,
}

#[derive(Debug)]
pub(crate) struct BatchInfo {
    pub replica_key: ReplicaKey,
    pub records: HashSet<RecordUid>,
}

impl BatchInfo {
    pub fn new(replica_key: ReplicaKey) -> Self {
        Self {
            replica_key,
            records: Default::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BatchSuccess {
    batch: BatchInfo,
    base_offset: Offset,
}

#[derive(Debug)]
pub(crate) struct BatchFailure {
    batch: BatchInfo,
    error_code: ErrorCode,
}

/// A type to build a `ProduceRequest` while associating the [`RecordUid`] of each record.
#[derive(Debug, Default)]
pub(crate) struct AssociatedRequest {
    pub request: ProduceRequest,
    pub uids: HashMap<ReplicaKey, BatchInfo>,
}

impl AssociatedRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, pending: AssociatedRecord) {
        let topic = {
            let maybe_topic = self
                .request
                .topics
                .iter_mut()
                .find(|t| t.name == pending.replica_key.topic);
            match maybe_topic {
                Some(t) => t,
                None => {
                    let topic = TopicProduceData {
                        name: pending.replica_key.topic.clone(),
                        ..Default::default()
                    };
                    self.request.topics.push(topic);
                    self.request.topics.last_mut().unwrap()
                }
            }
        };

        let partition = {
            let maybe_partition = topic
                .partitions
                .iter_mut()
                .find(|p| p.partition_index == pending.replica_key.partition);
            match maybe_partition {
                Some(p) => p,
                None => {
                    let partition = PartitionProduceData {
                        partition_index: pending.replica_key.partition,
                        ..Default::default()
                    };
                    topic.partitions.push(partition);
                    topic.partitions.last_mut().unwrap()
                }
            }
        };

        let batch = {
            let maybe_batch = partition.records.batches.first_mut();
            match maybe_batch {
                Some(b) => b,
                None => {
                    partition.records.batches.push(Batch::new());
                    partition.records.batches.last_mut().unwrap()
                }
            }
        };

        batch.add_record(pending.record);

        // Associate this record's UID according to its ReplicaKey
        let replica_key = pending.replica_key;
        self.uids
            .entry(replica_key.clone())
            .or_insert_with(|| BatchInfo::new(replica_key))
            .records
            .insert(pending.uid);
    }
}

pub(crate) struct AssociatedResponse {
    successes: Vec<BatchSuccess>,
    failures: Vec<BatchFailure>,
}

impl AssociatedResponse {
    pub fn new(
        response: ProduceResponse,
        mut batches: HashMap<ReplicaKey, BatchInfo>,
    ) -> Result<Self> {
        let mut successes = Vec::new();
        let mut failures = Vec::new();

        for topic in response.responses {
            for partition in topic.partitions {
                let replica_key = ReplicaKey::new(&topic.name, partition.partition_index);
                let batch = batches
                    .remove(&replica_key)
                    .ok_or_else(|| ProducerError::BatchNotFound)?;

                if partition.error_code.is_ok() {
                    let batch_success = BatchSuccess {
                        batch,
                        base_offset: partition.base_offset,
                    };
                    successes.push(batch_success);
                } else {
                    let batch_failure = BatchFailure {
                        batch,
                        error_code: partition.error_code,
                    };
                    failures.push(batch_failure);
                }
            }
        }

        Ok(Self {
            successes,
            failures,
        })
    }
}
