//!
//! # Connection actions
//!
//! Actions are received through check dispatcher channel and are forwarded to
//! Connection manager for processing.
//!
use flv_types::SpuId;
use flv_metadata::spu::SpuSpec;
use flv_metadata::partition::PartitionSpec;
use flv_metadata::partition::ReplicaKey;
use flv_metadata::topic::TopicSpec;
use flv_metadata::store::actions::*;
use flv_metadata::store::*;

use crate::stores::K8MetaItem;

/// Change in connection status
#[derive(Debug, PartialEq, Clone)]
pub enum SpuConnectionStatusChange {
    Off(SpuId),
    On(SpuId),
}

impl SpuConnectionStatusChange {
    pub fn spu_id(&self) -> SpuId {
        match self {
            SpuConnectionStatusChange::Off(id) => *id,
            SpuConnectionStatusChange::On(id) => *id,
        }
    }
}

impl std::fmt::Display for SpuConnectionStatusChange {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SpuConnectionStatusChange::Off(id) => write!(f, "SPU {} Off", id),
            SpuConnectionStatusChange::On(id) => write!(f, "SPU {} On", id),
        }
    }
}


/// Change sent to SPU
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionRequest {
    Spu(SpecChange<SpuSpec>),
    Partition(SpecChange<PartitionSpec>),
    None
}

impl ConnectionRequest {
    pub fn is_none(&self) -> bool {
        match self {
            Self::None =>  true,
            _ => false
        }
    }
}


impl From<LSChange<SpuSpec,K8MetaItem>> for ConnectionRequest {

    fn from(change: LSChange<SpuSpec,K8MetaItem>) -> Self {
        // only care about spu spec changes for spu request
        match change {
            LSChange::Add(new) => Self::Spu(SpecChange::Add(new.spec)),
            LSChange::Mod(new,old) => {
                if new.spec != old.spec {
                    Self::Spu(SpecChange::Mod(new.spec,old.spec))
                } else {
                    Self::None
                }
            },
            LSChange::Delete(old) => Self::Spu(SpecChange::Delete(old.spec))
        }
        
    }
}

impl From<LSChange<PartitionSpec,K8MetaItem>> for ConnectionRequest {

    fn from(change: LSChange<PartitionSpec,K8MetaItem>) -> Self {
        // only care about spu spec changes for spu request
        match change {
            LSChange::Add(new) => Self::Partition(SpecChange::Add(new.spec)),
            LSChange::Mod(new,old) => {
                if new.spec != old.spec {
                    Self::Partition(SpecChange::Mod(new.spec,old.spec))
                } else {
                    Self::None
                }
            },
            LSChange::Delete(old) => Self::Partition(SpecChange::Delete(old.spec))
        }
        
    }
}

impl From<LSChange<TopicSpec,K8MetaItem>> for ConnectionRequest {
    fn from(change: LSChange<TopicSpec,K8MetaItem>) -> Self {
        Self::None
    }
}

