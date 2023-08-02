mod spec;
mod status;

pub use self::spec::*;
pub use self::status::*;

#[cfg(feature = "k8")]
mod k8;
#[cfg(feature = "k8")]
pub use k8::*;

mod metadata {

    use fluvio_controlplane_metadata::core::{Spec, Status};

    use super::*;

    impl Spec for UpstreamClusterSpec {
        const LABEL: &'static str = "UpstreamCluster";
        type IndexKey = String;
        type Status = UpstreamClusterStatus;
        type Owner = Self;
    }

    impl Status for UpstreamClusterStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use fluvio_stream_model::{
            store::{
                k8::{K8ExtendedSpec, K8MetaItem, K8ConvertError, default_convert_from_k8},
                MetadataStoreObject,
            },
            k8_types::K8Obj,
        };

        use super::metadata::UpstreamClusterSpec;

        impl K8ExtendedSpec for UpstreamClusterSpec {
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
                status.into()
            }

            fn into_k8(self) -> Self::K8Spec {
                self.into()
            }
        }
    }
}
