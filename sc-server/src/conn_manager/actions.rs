//!
//! # Connection actions
//!
//! Actions are received through check dispatcher channel and are forwarded to
//! Connection manager for processing.
//!
use types::SpuId;
use metadata::spu::SpuSpec;
use metadata::partition::PartitionSpec;
use metadata::partition::ReplicaKey;

/// Change in connection status
#[derive(Debug,PartialEq,Clone)]
pub enum SpuConnectionStatusChange{
    Off(SpuId),
    On(SpuId)
}

impl SpuConnectionStatusChange {
    pub fn spu_id(&self) -> SpuId {
        match self {
            SpuConnectionStatusChange::Off(id) => *id,
            SpuConnectionStatusChange::On(id) => *id
        }
    }
}


impl std::fmt::Display for SpuConnectionStatusChange {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SpuConnectionStatusChange::Off(id) => 
                write!(f, "SPU {} Off",id),
            SpuConnectionStatusChange::On(id) => 
                write!(f, "SPU {} On",id)
        }

    }
}

#[derive(Debug, PartialEq)]
pub enum SpuSpecChange {
    Add(SpuSpec),                                // New Spec
    Mod(SpuSpec,SpuSpec),                     // Update SPU spec (new,old)
    Remove(SpuSpec),  
}

#[derive(Debug, PartialEq)]
pub enum PartitionSpecChange {
    Add(ReplicaKey,PartitionSpec),
    Mod(ReplicaKey,PartitionSpec,PartitionSpec),
    Remove(ReplicaKey,PartitionSpec)
}

/// Request to made to Connection Manager
#[derive(Debug, PartialEq)]
pub enum ConnectionRequest {
    Spu(SpuSpecChange),
    Partition(PartitionSpecChange),
    RefreshSpu(SpuId),                                       // Refresh SPU with it' metadata including SPU and Replica
}




