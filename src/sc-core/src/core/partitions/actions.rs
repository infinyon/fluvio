//!
//! # Partition Actions
//!
//! Defines Partition action requests
//!
//! # Remarks
//!
//! Partitions are created in 2 phases:
//! * Phase 1 - send spec to KV store (no status with replica map)
//! * Phase 2 - update status and send to KV store
//!
use std::fmt;

use flv_util::actions::Actions;
use internal_api::UpdateLrsRequest;

use crate::core::spus::SpuLSChange;
use crate::conn_manager::ConnectionRequest;

use super::PartitionWSAction;
use super::PartitionLSChange;

#[derive(Debug, PartialEq, Clone)]
pub enum PartitionChangeRequest {
    Partition(Actions<PartitionLSChange>),
    Spu(Actions<SpuLSChange>),
    LrsUpdate(UpdateLrsRequest),
}

impl fmt::Display for PartitionChangeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionChangeRequest::Partition(req) => write!(f, "Partition LS: {}", req.count()),
            PartitionChangeRequest::Spu(req) => write!(f, "SPU LS: {}", req.count()),
            PartitionChangeRequest::LrsUpdate(lrs) => write!(f, "Lrs Rep: {}", lrs.id),
        }
    }
}

#[derive(Debug, Default)]
pub struct PartitionActions {
    pub partitions: Actions<PartitionWSAction>,
    pub conns: Actions<ConnectionRequest>,
}

impl fmt::Display for PartitionActions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartitionActions partitions: {}, conns: {}",
            self.partitions.count(),
            self.conns.count()
        )
    }
}
