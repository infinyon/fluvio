mod spec;
mod status;

pub use self::spec::*;
pub use self::status::*;
pub use fluvio_protocol::record::ReplicaKey;

#[cfg(feature = "k8")]
mod k8;

mod metadata {

    use crate::partition::ReplicaKey;
    use crate::core::{Spec, Status};
    use crate::topic::TopicSpec;
    use crate::extended::{ObjectType, SpecExt};

    use super::*;

    impl Spec for PartitionSpec {
        const LABEL: &'static str = "Partition";
        type IndexKey = ReplicaKey;
        type Status = PartitionStatus;
        type Owner = TopicSpec;
    }

    impl SpecExt for PartitionSpec {
        const OBJECT_TYPE: ObjectType = ObjectType::Partition;
    }

    impl Status for PartitionStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use crate::store::k8::K8ExtendedSpec;
        use crate::store::k8::K8ConvertError;
        use crate::store::k8::K8MetaItem;
        use crate::store::MetadataStoreObject;
        use crate::k8_types::K8Obj;
        use crate::store::k8::default_convert_from_k8;

        use super::metadata::PartitionSpec;

        impl K8ExtendedSpec for PartitionSpec {
            type K8Spec = Self;

            const FINALIZER: Option<&'static str> =
                Some("partitions.finalizer.fluvio.infinyon.com");

            fn convert_from_k8(
                k8_obj: K8Obj<Self::K8Spec>,
                multi_namespace_context: bool,
            ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>
            {
                default_convert_from_k8(k8_obj, multi_namespace_context)
            }

            fn convert_status_from_k8(status: Self::Status) -> Self::Status {
                status
            }

            fn into_k8(self) -> Self::K8Spec {
                self
            }
        }
    }
}
