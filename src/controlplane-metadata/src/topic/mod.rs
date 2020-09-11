mod spec;
mod status;
pub mod store;

pub use self::spec::*;
pub use self::status::*;

pub const PENDING_REASON: &str = "waiting for live spus";

#[cfg(feature = "k8")]
mod k8;
#[cfg(feature = "k8")]
pub use k8::*;

mod metadata {

    use crate::core::*;
    use super::*;

    impl Spec for TopicSpec {
        const LABEL: &'static str = "Topic";
        type IndexKey = String;
        type Status = TopicStatus;
        type Owner = Self;
    }

    impl Removable for TopicSpec {
        type DeleteKey = String;
    }

    impl Creatable for TopicSpec {}

    impl Status for TopicStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use crate::store::k8::K8ExtendedSpec;
        use crate::store::k8::K8ConvertError;
        use crate::store::k8::K8MetaItem;
        use crate::store::MetadataStoreObject;
        use crate::k8::metadata::K8Obj;
        use crate::store::k8::default_convert_from_k8;

        use super::TopicSpec;

        impl K8ExtendedSpec for TopicSpec {
            type K8Spec = Self;
            type K8Status = Self::Status;

            fn convert_from_k8(
                k8_obj: K8Obj<Self::K8Spec>,
            ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>
            {
                default_convert_from_k8(k8_obj)
            }
        }
    }
}
