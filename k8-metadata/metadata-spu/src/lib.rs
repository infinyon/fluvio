mod spec;
mod status;

pub use self::spec::SpuSpec;
pub use self::spec::SpuType;
pub use self::spec::Endpoint;
pub use self::spec::EncryptionEnum;

pub use self::status::SpuStatus;
pub use self::status::SpuStatusResolution;

use metadata_core::Crd;
use metadata_core::CrdNames;
use metadata_core::GROUP;
use metadata_core::V1;

const SPU_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Spu",
        plural: "spus",
        singular: "spu",
    },
};
