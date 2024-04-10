mod spec;
mod status;

pub use self::spec::*;
pub use self::status::*;

#[cfg(feature = "k8")]
mod k8;

mod metadata {

    use crate::{
        core::{Spec, Status},
        extended::{ObjectType, SpecExt},
    };

    use super::*;

    impl Spec for RemoteSpec {
        const LABEL: &'static str = "Remote";
        type IndexKey = String;
        type Status = RemoteStatus;
        type Owner = Self;
    }

    impl SpecExt for RemoteSpec {
        const OBJECT_TYPE: ObjectType = ObjectType::Remote;
    }

    impl Status for RemoteStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use fluvio_stream_model::{
            store::{
                k8::{K8ExtendedSpec, K8MetaItem, K8ConvertError, default_convert_from_k8},
                MetadataStoreObject,
            },
            k8_types::K8Obj,
        };

        use super::metadata::RemoteSpec;

        impl K8ExtendedSpec for RemoteSpec {
            type K8Spec = Self;

            fn convert_from_k8(
                k8_obj: K8Obj<Self::K8Spec>,
                multi_namespace_context: bool,
            ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>
            {
                default_convert_from_k8(k8_obj, multi_namespace_context)
            }

            fn convert_status_from_k8(
                status: Self::Status,
            ) -> <Self::K8Spec as fluvio_stream_model::k8_types::Spec>::Status {
                status
            }

            fn into_k8(self) -> Self::K8Spec {
                self
            }
        }
    }
}
