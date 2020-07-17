mod spec;
mod status;
mod policy;
mod replica;
pub mod store;

pub use self::spec::*;
pub use self::status::*;
pub use kf_protocol::api::ReplicaKey;
pub use self::policy::*;
pub use self::replica::*;

#[cfg(feature = "k8")]
mod k8;
#[cfg(feature = "k8")]
pub use k8::*;

mod metadata {

    use crate::partition::ReplicaKey;
    use crate::core::*;
    use crate::topic::TopicSpec;
    use super::*;

    impl Spec for PartitionSpec {
        const LABEL: &'static str = "Partition";
        type IndexKey = ReplicaKey;
        type Status = PartitionStatus;
        type Owner = TopicSpec;
    }

    impl Status for PartitionStatus {}
}