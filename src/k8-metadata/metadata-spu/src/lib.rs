mod spec;
mod status;

pub use self::spec::*;
pub use self::status::*;

use k8_obj_metadata::Crd;
use k8_obj_metadata::CrdNames;
use k8_obj_metadata::GROUP;
use k8_obj_metadata::V1;

const SPU_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Spu",
        plural: "spus",
        singular: "spu",
    },
};
