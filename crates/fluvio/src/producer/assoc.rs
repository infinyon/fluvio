//! Associate unique records with the requests and responses they are delivered on.
//!
//! This module contains types to help answer the question: "has my record been
//! committed successfully or not"?

use std::collections::HashMap;
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

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BatchInfo {
    pub replica_key: ReplicaKey,
    pub records: Vec<RecordUid>,
}

impl BatchInfo {
    pub fn new(replica_key: ReplicaKey) -> Self {
        Self {
            replica_key,
            records: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum BatchStatus {
    Success(BatchSuccess),
    Failure(BatchFailure),
    InternalError(String),
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) struct BatchSuccess {
    pub batch: BatchInfo,
    pub base_offset: Offset,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BatchFailure {
    pub batch: BatchInfo,
    pub error_code: ErrorCode,
}

/// A type to build a `ProduceRequest` while associating the [`RecordUid`] of each record.
#[derive(Debug, Default)]
pub(crate) struct AssociatedRequest {
    pub(crate) request: ProduceRequest,
    pub(crate) uids: HashMap<ReplicaKey, BatchInfo>,
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
            .push(pending.uid);
    }
}

pub(crate) struct AssociatedResponse {
    pub successes: Vec<BatchSuccess>,
    pub failures: Vec<BatchFailure>,
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
                    .ok_or(ProducerError::BatchNotFound)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use dataplane::record::RecordKey;

    #[test]
    fn test_associated_request() {
        let mut request = AssociatedRequest::new();

        request.add(AssociatedRecord {
            uid: RecordUid(0),
            replica_key: ReplicaKey::new("One", 0),
            record: Record::from((RecordKey::NULL, "0")),
        });
        assert!(request
            .uids
            .get(&ReplicaKey::new("One", 0))
            .unwrap()
            .records
            .contains(&RecordUid(0)));

        request.add(AssociatedRecord {
            uid: RecordUid(1),
            replica_key: ReplicaKey::new("One", 0),
            record: Record::from((RecordKey::NULL, "1")),
        });

        request.add(AssociatedRecord {
            uid: RecordUid(2),
            replica_key: ReplicaKey::new("One", 1),
            record: Record::from((RecordKey::NULL, "2")),
        });

        let output = request.request;

        let one = &output.topics[0];
        assert_eq!(one.name, "One");

        let one_0 = &one.partitions[0];
        assert_eq!(one_0.partition_index, 0);
        assert_eq!(one_0.records.batches.len(), 1);

        let one_0_0 = &one_0.records.batches[0].records()[0];
        assert_eq!(one_0_0.key, None);
        assert_eq!(one_0_0.value, "0".into());

        let one_0_1 = &one_0.records.batches[0].records()[1];
        assert_eq!(one_0_1.key, None);
        assert_eq!(one_0_1.value, "1".into());

        let one_1 = &one.partitions[1];
        let one_1_0 = &one_1.records.batches[0].records()[0];
        assert_eq!(one_1_0.key, None);
        assert_eq!(one_1_0.value, "2".into());
    }
}
