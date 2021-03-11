use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use k8_types::core::service::{ ServiceSpec as K8ServiceSpec, ServiceStatus as K8ServiceStatus};
use k8_types::core::service::LoadBalancerIngress;

use crate::dispatcher::core::Spec;
use crate::dispatcher::core::Status;
use crate::stores::spg::SpuGroupSpec;

/// Service associated with SPU
#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpuServicespec(K8ServiceSpec);

impl Spec for SpuServicespec {
    const LABEL: &'static str = "SpuService";
    type IndexKey = String;
    type Status = SpuServiceStatus;
    type Owner = SpuGroupSpec;
}

impl SpuServicespec {
    pub fn inner(&self) -> &K8ServiceSpec {
        &self.0
    }
}

impl From<K8ServiceSpec> for SpuServicespec {
    fn from(k8: K8ServiceSpec) -> Self {
        Self(k8)
    }
}

impl From<SpuServicespec> for K8ServiceSpec {
    fn from(spec: SpuServicespec) -> Self {
        spec.0
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
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

    use super::*;


    impl K8ExtendedSpec for SpuServicespec {
        type K8Spec = ServiceSpec;
        type K8Status = ServiceStatus;

        fn convert_from_k8(
            k8_obj: K8Obj<Self::K8Spec>,
        ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>> {
            use std::convert::TryInto;
            use std::io::Error as IoError;
            use std::io::ErrorKind;

            let labels = &k8_obj.metadata.labels;

            if let Some(name) = labels.get("fluvio.io/spu-name") {
                debug!(spu = %name,
                    service_name = %k8_obj.metadata.name,
                    "detected spu service");
                trace!("converting k8 spu service: {:#?}", k8_obj);

                let ctx_result: Result<K8MetaItem, _> = k8_obj.metadata.clone().try_into();
                match ctx_result {
                    Ok(ctx_item) => {
                        let mut meta = MetadataStoreObject::new(
                            name,
                            SpuServicespec(
                                k8_obj.spec
                            ),
                            SpuServiceStatus(k8_obj.status),
                        );
                        meta.set_ctx(ctx_item.into());
                        Ok(meta)
                    }
                    Err(err) => Err(K8ConvertError::KeyConvertionError(IoError::new(
                        ErrorKind::InvalidData,
                        format!("error converting metadata: {:#?}", err),
                    ))),
                }
            } else {
                trace!(
                    name = %k8_obj.metadata.name,
                    "skipping non spu service");
                Err(K8ConvertError::Skip(k8_obj))
            }
        }
    }
}
