mod spec;
mod status;

pub use self::spec::TopicSpec;
pub use self::spec::PartitionMap;
pub use self::spec::PartitionMaps;
pub use self::spec::TopicReplicaParam;

pub use self::status::TopicStatus;
pub use self::status::TopicResolution;

pub const PENDING_REASON: &'static str = "waiting for live spus";
