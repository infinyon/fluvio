//!
//! # Partition Spec
//!
//! Interface to the Partition metadata spec in K8 key value store
//!
use crate::PARTITION_API;
use k8_obj_metadata::Crd;
use k8_obj_metadata::Spec;
use k8_obj_metadata::DefaultHeader;

use serde::Deserialize;
use serde::Serialize;

use super::PartitionStatus;

impl Spec for PartitionSpec {
    type Header = DefaultHeader;
    type Status = PartitionStatus;
    fn metadata() -> &'static Crd {
        &PARTITION_API
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PartitionSpec {
    pub leader: i32,
    pub replicas: Vec<i32>,
}

impl PartitionSpec {
    pub fn new(leader: i32, replicas: Vec<i32>) -> Self {
        PartitionSpec { leader, replicas }
    }
}
