mod spec;
mod status;

pub use self::spec::TopicSpec;
pub use self::spec::Partition;

pub use self::status::TopicStatus;
pub use self::status::TopicStatusResolution;

use k8_obj_metadata::Crd;
use k8_obj_metadata::CrdNames;
use k8_obj_metadata::GROUP;
use k8_obj_metadata::V1;

const TOPIC_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "Topic",
        plural: "topics",
        singular: "topic",
    },
};
