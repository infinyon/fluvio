use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use k8_types::{
    ObjectMeta,
    core::service::{ServiceSpec as K8ServiceSpec, ServiceStatus as K8ServiceStatus},
};
use k8_types::core::service::LoadBalancerIngress;

use crate::dispatcher::core::{Spec, Status};
use crate::stores::spg::SpuGroupSpec;

/// Service associated with SPU
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpuServiceSpec(K8ServiceSpec);

impl Spec for SpuServiceSpec {
    const LABEL: &'static str = "SpuService";
    type IndexKey = String;
    type Status = SpuServiceStatus;
    type Owner = SpuGroupSpec;
}

impl SpuServiceSpec {
    pub fn inner(&self) -> &K8ServiceSpec {
        &self.0
    }

    /// unique name given spu name
    pub fn service_name(spu_name: &str) -> String {
        format!("fluvio-spu-{spu_name}")
    }

    pub fn spu_name(meta: &ObjectMeta) -> Option<&String> {
        meta.labels.get("fluvio.io/spu-name")
    }

    pub fn ingress_annotation(meta: &ObjectMeta) -> Option<&String> {
        meta.annotations.get("fluvio.io/ingress-address")
    }
}

impl From<K8ServiceSpec> for SpuServiceSpec {
    fn from(k8: K8ServiceSpec) -> Self {
        Self(k8)
    }
}

impl From<SpuServiceSpec> for K8ServiceSpec {
    fn from(spec: SpuServiceSpec) -> Self {
        spec.0
    }
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Default, Clone)]
pub struct SpuServiceStatus(K8ServiceStatus);

impl fmt::Display for SpuServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}

impl Status for SpuServiceStatus {}

impl SpuServiceStatus {
    pub fn ingress(&self) -> &Vec<LoadBalancerIngress> {
        &self.0.load_balancer.ingress
    }
}

impl From<K8ServiceStatus> for SpuServiceStatus {
    fn from(k8: K8ServiceStatus) -> Self {
        Self(k8)
    }
}

impl From<SpuServiceStatus> for K8ServiceStatus {
    fn from(status: SpuServiceStatus) -> Self {
        status.0
    }
}

mod extended {

    // use std::io::Error as IoError;
    // use std::io::ErrorKind;

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

    impl K8ExtendedSpec for SpuServiceSpec {
        type K8Spec = ServiceSpec;
        type K8Status = ServiceStatus;

        fn convert_from_k8(
            k8_obj: K8Obj<Self::K8Spec>,
            multi_namespace_context: bool,
        ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>> {
            if let Some(name) = SpuServiceSpec::spu_name(&k8_obj.metadata) {
                debug!(spu = %name,
                    service_name = %k8_obj.metadata.name,
                    "detected spu service");
                trace!("converting k8 spu service: {:#?}", k8_obj);

                default_convert_from_k8(k8_obj, multi_namespace_context)
            } else {
                trace!(
                    name = %k8_obj.metadata.name,
                    "skipping non spu service");
                Err(K8ConvertError::Skip(Box::new(k8_obj)))
            }
        }
    }
}
