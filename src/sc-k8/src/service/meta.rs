use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::dispatcher::core::Spec;
use crate::dispatcher::core::Status;
use crate::k8::core::service::ServiceStatus;
use crate::k8::core::service::LoadBalancerIngress;

/// Service associated with SPU
#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpuServicespec {
    pub spu_name: String
}

impl Spec for SpuServicespec {
    const LABEL: &'static str = "SpuService";
    type IndexKey = String;
    type Status = SpuServiceStatus;
    type Owner = Self;
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
pub struct SpuServiceStatus(ServiceStatus);

impl fmt::Display for SpuServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.0)
    }
}

impl Status for SpuServiceStatus {}


impl SpuServiceStatus  {
    pub fn ingress(&self) -> &Vec<LoadBalancerIngress> {
        &self.0.load_balancer.ingress
    }
}


mod extended {

   // use std::io::Error as IoError;
   // use std::io::ErrorKind;

    use tracing::debug;

    use crate::k8::core::service::ServiceSpec;
    use crate::k8::core::service::ServiceStatus;
    use crate::k8::metadata::K8Obj;
    use crate::stores::k8::K8ConvertError;
    use crate::stores::k8::K8ExtendedSpec;
    use crate::stores::k8::K8MetaItem;
    use crate::stores::MetadataStoreObject;


    use super::*;

    /*
    // to satisfy bounds for k8 dispatcher ws service
    impl Into<SecretSpec> for InstallSpec {
        fn into(self) -> SecretSpec {
            panic!("use helm to install");
        }
    }

    impl Into<SecretStatus> for InstallStatus {
        fn into(self) -> SecretStatus {
            panic!("use helm to install");
        }
    }*/


    impl K8ExtendedSpec for SpuServicespec {
        type K8Spec = ServiceSpec;
        type K8Status = ServiceStatus;

        fn convert_from_k8(
            k8_obj: K8Obj<Self::K8Spec>,
        ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>> {


            let labels = &k8_obj.metadata.labels;

            if let Some(name) = labels.get("fluvio.io/spu-name") {

                    debug!("detected spu service: {}", name);

                    Ok(MetadataStoreObject::new(name, SpuServicespec{
                        spu_name: name.to_owned()
                    }, SpuServiceStatus(k8_obj.status)))
            } else {
                    debug!("skipping non acct fluvio {}", k8_obj.metadata.namespace);
                    Err(K8ConvertError::Skip(k8_obj))
            }
            
        }
    }
}

/*
mod k8 {
    use std::io::Error as IoError;
    use std::ops::Deref;
    use std::sync::Arc;

    use tracing::debug;
    use tracing::trace;
    use serde::Deserialize;
    use serde::Serialize;

    use k8_obj_metadata::Crd;
    use k8_obj_metadata::CrdNames;

    use flv_operator::MetaItem;
    use flv_operator::MetaItemLocalStore;
    use flv_operator::StoreSpec;
    use k8_obj_core::secret::SecretSpec;
    use k8_obj_core::secret::SecretStatus;
    use k8_obj_metadata::store::MetaItemContext;
    use k8_obj_metadata::DefaultHeader;
    use k8_obj_metadata::K8Obj;
    use k8_obj_metadata::Spec;
    use k8_obj_metadata::Status;
    use k8_obj_core::service::ServiceSpec;


    use crate::k8::GROUP;
    use crate::k8::V1;

    pub type InstallLocalStore = MetaItemLocalStore<HelmInstallSpec>;
    pub type SharedInstallLocalStore = Arc<InstallLocalStore>;
    pub type HelmInstallItem = MetaItem<HelmInstallSpec>;

    pub type SecretLocalStore = MetaItemLocalStore<SecretWrapperSpec>;
    pub type SharedSecreteLocalStore = Arc<SecretLocalStore>;
    pub type SecreteItem = MetaItem<SecretWrapperSpec>;

    pub type ServiceLocalStore = MetaItemLocalStore<ServiceSpec>;

    const INSTALL_CRD: Crd = Crd {
        group: GROUP,
        version: V1,
        names: CrdNames {
            kind: "Installation",
            plural: "installations",
            singular: "installation",
        },
    };


    #[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
    pub struct HelmInstallSpec {
        pub namespace: String,
    }

    impl Spec for HelmInstallSpec {
        type Status = HelmInstallStatus;
        type Header = DefaultHeader;

        fn metadata() -> &'static Crd {
            &INSTALL_CRD
        }
    }

    impl StoreSpec for HelmInstallSpec {
        const LABEL: &'static str = "Install";

        type K8Spec = Self;
        type Status = HelmInstallStatus;
        type Key = String;
        type Owner = Self;

        fn convert_from_k8(k8_obj: K8Obj<Self::K8Spec>) -> Result<Option<MetaItem<Self>>, IoError> {
            let ctx = MetaItemContext::default().with_ctx(k8_obj.metadata.clone());
            Ok(Some(MetaItem::new(
                k8_obj.metadata.name,
                k8_obj.spec,
                k8_obj.status,
                ctx,
            )))
        }
    }

    #[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
    pub struct SecretWrapperSpec(SecretSpec);

    impl Deref for SecretWrapperSpec {
        type Target = SecretSpec;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl StoreSpec for SecretWrapperSpec {
        const LABEL: &'static str = "Secret";

        type K8Spec = SecretSpec;
        type Status = SecretStatus;
        type Key = String;
        type Owner = Self;

        fn convert_from_k8(k8_obj: K8Obj<Self::K8Spec>) -> Result<Option<MetaItem<Self>>, IoError> {
            // check if type if helm installation
            if k8_obj.header.ty == "helm.sh/release.v1" {
                let mut labels = &k8_obj.metadata.labels;
                // let status = labels.remove("status").unwrap();
                // let version = labels.remove("version").unwrap();
                let name = labels.get("name").as_ref().unwrap().clone();

                if name == "fluvio-sys" {
                    debug!("ignoring system chart");
                    Ok(None)
                } else {
                    let key = k8_obj.metadata.namespace.clone();
                    debug!(
                        "found helm secret: {} at: {}",
                        name, key,
                    );
                    let ctx = MetaItemContext::default().with_ctx(k8_obj.metadata.clone());
                    Ok(Some(MetaItem::new(
                        key,
                        SecretWrapperSpec(k8_obj.spec),
                        k8_obj.status,
                        ctx,
                    )))
                }


            } else {
                trace!(
                    "ignoring secret: {}, {:#?}",
                    k8_obj.metadata.name,
                    k8_obj.header
                );
                Ok(None)
            }
        }
    }

    #[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
    pub enum InstallResolution {
        Init,
        Provisioned,
        Installed,
    }

    impl Default for InstallResolution {
        fn default() -> Self {
            Self::Init
        }
    }

    #[derive(Deserialize, Serialize, Debug, PartialEq, Default, Clone)]
    pub struct HelmInstallStatus {
        pub resolution: InstallResolution,
        pub public_host: Option<String>

    }

    impl Status for HelmInstallStatus {}
}
*/
