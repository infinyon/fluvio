//!
//! # Cluster
//!
//! Interface to the Cluster metadata in K8 key value store
//!
mod spec;

pub use self::spec::*;

use super::TableStatus;
use crate::k8_types::Status as K8Status;

/// implement k8 status for table status because they are same
impl K8Status for TableStatus {}
