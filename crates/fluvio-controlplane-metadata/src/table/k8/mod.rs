//!
//! # Cluster
//!
//! Interface to the Cluster metadata in K8 key value store
//!

//mod spec;
//pub use self::spec::*;

use super::TableStatus;
use super::TableSpec;
use crate::k8_types::Status as K8Status;
use crate::k8_types::{Crd, Spec, DefaultHeader};

/// implement k8 status for table status because they are same
impl K8Status for TableStatus {}

use crd::TABLE_SPEC_API;
mod crd {

    use crate::k8_types::{Crd, CrdNames, GROUP, V1};

    pub const TABLE_SPEC_API: Crd = Crd {
        group: GROUP,
        version: V1,
        names: CrdNames {
            kind: "Table",
            plural: "tables",
            singular: "table",
        },
    };
}

impl Spec for TableSpec {
    type Status = TableStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &TABLE_SPEC_API
    }
}
