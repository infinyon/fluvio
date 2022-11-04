use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use k8_types::core::service::{ServiceSpec as K8ServiceSpec, ServiceStatus as K8ServiceStatus};

use crate::dispatcher::core::Spec;
use crate::dispatcher::core::Status;
use crate::stores::spg::SpuGroupSpec;

/// Service associated with SPU
#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpgServiceSpec(K8ServiceSpec);

impl SpgServiceSpec {
    pub fn inner(&self) -> &K8ServiceSpec {
        &self.0
    }
}

impl Spec for SpgServiceSpec {
    const LABEL: &'static str = "SpgService";
    type IndexKey = String;
    type Status = SpgServiceStatus;
    type Owner = SpuGroupSpec;
}

impl From<K8ServiceSpec> for SpgServiceSpec {
    fn from(k8: K8ServiceSpec) -> Self {
        Self(k8)
    }
}

impl From<SpgServiceSpec> for K8ServiceSpec {
    fn from(spec: SpgServiceSpec) -> Self {
        spec.0
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Default, Clone)]
pub struct SpgServiceStatus(K8ServiceStatus);

impl fmt::Display for SpgServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}

impl Status for SpgServiceStatus {}

impl From<K8ServiceStatus> for SpgServiceStatus {
    fn from(k8: K8ServiceStatus) -> Self {
        Self(k8)
    }
}

impl From<SpgServiceStatus> for K8ServiceStatus {
    fn from(status: SpgServiceStatus) -> Self {
        status.0
    }
}

mod extended {

    use tracing::debug;
    use tracing::trace;

    use k8_types::core::service::ServiceSpec;
    use k8_types::core::service::ServiceStatus;
    use k8_types::K8Obj;

    use crate::stores::k8::K8ConvertError;
    use crate::stores::k8::K8ExtendedSpec;
    use crate::stores::k8::K8MetaItem;
    use crate::stores::MetadataStoreObject;
    use crate::stores::k8::default_convert_from_k8;

    use super::*;

    impl K8ExtendedSpec for SpgServiceSpec {
        type K8Spec = ServiceSpec;
        type K8Status = ServiceStatus;

        fn convert_from_k8(
            k8_obj: K8Obj<Self::K8Spec>,
            multi_namespace_context: bool,
        ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>> {
            if k8_obj.metadata.name.starts_with("fluvio-spg") {
                debug!(name = %k8_obj.metadata.name,
                    "detected spg service");
                trace!("converting k8 spu service: {:#?}", k8_obj);

                default_convert_from_k8(k8_obj, multi_namespace_context)
            } else {
                trace!(
                    name = %k8_obj.metadata.name,
                    "skipping non spg service");
                Err(K8ConvertError::Skip(Box::new(k8_obj)))
            }
        }
    }
}
