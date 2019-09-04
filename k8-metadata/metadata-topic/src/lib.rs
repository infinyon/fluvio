mod spec;
mod status;

pub use self::spec::TopicSpec;
pub use self::spec::Partition;

pub use self::status::TopicStatus;
pub use self::status::TopicStatusResolution;

use metadata_core::Crd;
use metadata_core::CrdNames;
use metadata_core::GROUP;
use metadata_core::V1;

const TOPIC_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Topic",
        plural: "topics",
        singular: "topic",
    },
};
