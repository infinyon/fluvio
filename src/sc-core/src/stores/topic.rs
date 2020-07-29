pub use flv_metadata_cluster::topic::store::*;
pub use flv_metadata_cluster::topic::*;

use super::*;

pub type TopicAdminStore = TopicLocalStore<K8MetaItem>;
pub type TopicAdminMd = TopicMetadata<K8MetaItem>;

impl K8ExtendedSpec for TopicSpec {
    type K8Spec = Self;
    type K8Status = Self::Status;
}
