//!
//! # Cluster
//!
//! Interface to the Cluster metadata in K8 key value store
//!

//mod spec;
//pub use self::spec::*;

use super::TableFormatStatus;
use super::TableFormatSpec;
use crate::k8_types::Status as K8Status;
use crate::k8_types::{Crd, Spec, DefaultHeader};

/// implement k8 status for tableformat status because they are same
impl K8Status for TableFormatStatus {}

use crd::TABLEFORMAT_SPEC_API;
mod crd {

    use crate::k8_types::{Crd, CrdNames, GROUP, V1};

    pub const TABLEFORMAT_SPEC_API: Crd = Crd {
        group: GROUP,
        version: V1,
        names: CrdNames {
            kind: "TableFormat",
            plural: "tableformats",
            singular: "tableformat",
        },
    };
}

impl Spec for TableFormatSpec {
    type Status = TableFormatStatus;
    type Header = DefaultHeader;

    fn metadata() -> &'static Crd {
        &TABLEFORMAT_SPEC_API
    }
}
