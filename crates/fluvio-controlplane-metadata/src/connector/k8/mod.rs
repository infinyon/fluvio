//!
//! # Cluster
//!
//! Interface to the Cluster metadata in K8 key value store
//!
mod spec;

pub use self::spec::*;

use super::ManagedConnectorStatus;
use crate::k8_types::Status as K8Status;

/// implement k8 status for managed connector status because they are same
impl K8Status for ManagedConnectorStatus {}
