//!
//! # Cluster
//!
//! Interface to the Cluster metadata in K8 key value store
//!
mod spec;
mod status;

pub use self::spec::*;
pub use self::status::*;

use metadata_core::Crd;
use metadata_core::CrdNames;
use metadata_core::GROUP;
use metadata_core::V1;

const SPG_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "SpuGroup",
        plural: "spugroups",
        singular: "spugroup",
    },
};
