#![allow(clippy::assign_op_pattern)]

use std::fmt;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_types::SpuId;

use crate::partition::ReplicaKey;
use crate::core::{MetadataItem};
use crate::store::MetadataStoreObject;
use crate::partition::PartitionSpec;
use crate::topic::{CleanupPolicy, TopicStorageConfig, CompressionAlgorithm};
use super::store::*;

/// Metadata about Replica send from SC
#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct Replica {
    pub id: ReplicaKey,
    pub leader: SpuId,
    pub replicas: Vec<SpuId>,
    pub is_being_deleted: bool,
    pub cleanup_policy: Option<CleanupPolicy>,
    pub storage: Option<TopicStorageConfig>,
    pub compression_type: CompressionAlgorithm,
}

impl Replica {
    pub fn new(id: impl Into<ReplicaKey>, leader: SpuId, replicas: Vec<SpuId>) -> Self {
        Self::new_with_delete(id.into(), leader, replicas, false)
    }

    pub fn new_with_delete(
        id: ReplicaKey,
        leader: SpuId,
        replicas: Vec<SpuId>,
        is_being_deleted: bool,
    ) -> Self {
        Self {
            id,
            leader,
            replicas,
            is_being_deleted,
            ..Default::default()
        }
    }
}

impl<C> From<PartitionMetadata<C>> for Replica
where
    C: MetadataItem,
{
    fn from(item: PartitionMetadata<C>) -> Self {
        let inner: MetadataStoreObject<PartitionSpec, C> = item;
        // consider either metadata ctx is deleted or status is deleted
        let is_being_deleted =
            inner.status.is_being_deleted || inner.ctx().item().is_being_deleted();

        let spec = inner.spec;
        Self {
            id: inner.key,
            leader: spec.leader,
            replicas: spec.replicas,
            is_being_deleted,
            cleanup_policy: spec.cleanup_policy,
            storage: spec.storage,
            compression_type: spec.compression_type,
        }
    }
}

impl fmt::Display for Replica {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} leader: {} replicas: [", self.id, self.leader)?;
        for replica in &self.replicas {
            write!(f, "{replica},")?;
        }
        write!(f, "]")?;
        if self.is_being_deleted {
            write!(f, " is_deleted")
        } else {
            write!(f, "")
        }
    }
}

/// given replica, where is leader
#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct ReplicaLeader {
    pub id: ReplicaKey,
    pub leader: SpuId,
}
