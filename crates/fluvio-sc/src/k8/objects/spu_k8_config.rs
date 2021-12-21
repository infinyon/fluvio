use std::collections::{BTreeMap, HashMap};
use std::fmt;

use fluvio_types::defaults::SPU_PUBLIC_PORT;
use tracing::{debug, info};
use serde::{Deserialize};

use k8_client::{ClientError};

use k8_types::core::pod::{ResourceRequirements, PodSecurityContext};
use k8_types::core::config_map::{ConfigMapSpec, ConfigMapStatus};
use k8_types::core::service::{ServicePort, ServiceSpec, TargetPort, LoadBalancerType};
use fluvio_controlplane_metadata::core::MetadataContext;

use crate::dispatcher::core::{Spec, Status};

const CONFIG_MAP_NAME: &str = "spu-k8";

// this is same struct as in helm config
#[derive(Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct PodConfig {
    #[serde(default)]
    pub node_selector: HashMap<String, String>,
    pub resources: Option<ResourceRequirements>,
    pub storage_class: Option<String>,
    pub base_node_port: Option<u16>,
}

#[derive(Debug, PartialEq, Default, Clone)]
pub struct ScK8Config {
    pub image: String,
    pub pod_security_context: Option<PodSecurityContext>,
    pub lb_service_annotations: HashMap<String, String>,
    pub service: Option<ServiceSpec>,
    pub spu_pod_config: PodConfig,
    pub connector_prefixes: Vec<String>,
}

impl ScK8Config {
    fn from(mut data: BTreeMap<String, String>) -> Result<Self, ClientError> {
        debug!("ConfigMap {} data: {:?}", CONFIG_MAP_NAME, data);

        let image = data.remove("image").ok_or_else(|| {
            ClientError::Other("image not found in ConfigMap spu-k8 data".to_owned())
        })?;

        let pod_security_context =
            if let Some(pod_security_context_string) = data.remove("podSecurityContext") {
                serde_json::from_str(&pod_security_context_string)?
            } else {
                None
            };

        let lb_service_annotations =
            if let Some(lb_service_annotations) = data.remove("lbServiceAnnotations") {
                serde_json::from_str(&lb_service_annotations)?
            } else {
                HashMap::new()
            };

        let service = if let Some(service_data) = data.remove("service") {
            Some(serde_json::from_str(&service_data)?)
        } else {
            None
        };

        let spu_pod_config = if let Some(config_str) = data.remove("spuPodConfig") {
            serde_json::from_str(&config_str).map_err(|err| {
                ClientError::Other(format!("not able to parse spu pod config: {:#?}", err))
            })?
        } else {
            info!("spu pod config not found, using default");
            PodConfig::default()
        };

        info!(?spu_pod_config, "spu pod config");

        let connector_prefixes: Vec<String> =
            if let Some(prefix_string) = data.remove("connectorPrefixes") {
                prefix_string.split(' ').map(|s| s.to_owned()).collect()
            } else {
                Vec::new()
            };

        Ok(Self {
            image,
            pod_security_context,
            lb_service_annotations,
            service,
            spu_pod_config,
            connector_prefixes,
        })
    }

    /*
    pub async fn load(client: &SharedK8Client, namespace: &str) -> Result<Self, ClientError> {
        let meta = InputObjectMeta::named(CONFIG_MAP_NAME, namespace);
        let k8_obj = client.retrieve_item::<ConfigMapSpec, _>(&meta).await?;

        Self::from(k8_obj.header.data)

    }
    */

    /// apply service config to service
    pub fn apply_service(&self, replica: u16, k8_service: &mut ServiceSpec) {
        let mut public_port = ServicePort {
            port: SPU_PUBLIC_PORT,
            ..Default::default()
        };
        public_port.target_port = Some(TargetPort::Number(public_port.port));

        if let Some(service_template) = &self.service {
            if let Some(ty) = &service_template.r#type {
                k8_service.r#type = Some(ty.clone());
                if let (LoadBalancerType::NodePort, Some(node_port)) =
                    (ty, self.spu_pod_config.base_node_port)
                {
                    public_port.node_port = Some(node_port + replica);
                };
            }
            if let Some(local_traffic) = &service_template.external_traffic_policy {
                k8_service.external_traffic_policy = Some(local_traffic.clone());
            }
            if let Some(external_name) = &service_template.external_name {
                k8_service.external_name = Some(external_name.clone());
            }
            if let Some(lb_ip) = &k8_service.load_balancer_ip {
                k8_service.load_balancer_ip = Some(lb_ip.clone());
            }
        }

        k8_service.ports = vec![public_port];
    }
}

impl Spec for ScK8Config {
    const LABEL: &'static str = "FluvioConfig";
    type IndexKey = String;
    type Status = FluvioConfigStatus;
    type Owner = Self;
}

impl From<ConfigMapSpec> for ScK8Config {
    fn from(_spec: ConfigMapSpec) -> Self {
        panic!("can't do it")
    }
}

impl From<ScK8Config> for ConfigMapSpec {
    fn from(_spec: ScK8Config) -> Self {
        panic!("can't do it")
    }
}

#[derive(Deserialize, Debug, PartialEq, Default, Clone)]
pub struct FluvioConfigStatus();

impl Status for FluvioConfigStatus {}

impl fmt::Display for FluvioConfigStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "")
    }
}

impl From<ConfigMapStatus> for FluvioConfigStatus {
    fn from(_k8: ConfigMapStatus) -> Self {
        Self {}
    }
}

impl From<FluvioConfigStatus> for ConfigMapStatus {
    fn from(_status: FluvioConfigStatus) -> Self {
        panic!("can't do it")
    }
}

mod extended {

    use std::convert::TryInto;
    use std::io::Error as IoError;
    use std::io::ErrorKind;

    use tracing::debug;
    use tracing::trace;

    use k8_types::K8Obj;
    use k8_types::core::config_map::{ConfigMapSpec, ConfigMapStatus};

    use crate::stores::k8::K8ConvertError;
    use crate::stores::k8::K8ExtendedSpec;
    use crate::stores::k8::K8MetaItem;
    use crate::stores::{MetadataStoreObject};

    use super::*;

    impl K8ExtendedSpec for ScK8Config {
        type K8Spec = ConfigMapSpec;
        type K8Status = ConfigMapStatus;

        fn convert_from_k8(
            k8_obj: K8Obj<Self::K8Spec>,
        ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>> {
            if k8_obj.metadata.name == "spu-k8" {
                debug!(k8_name = %k8_obj.metadata.name,
                    "detected fluvio config");
                trace!("converting k8 spu service: {:#?}", k8_obj);

                match ScK8Config::from(k8_obj.header.data) {
                    Ok(config) => match k8_obj.metadata.try_into() {
                        Ok(ctx_item) => {
                            let ctx = MetadataContext::new(ctx_item, None);
                            Ok(
                                MetadataStoreObject::new("fluvio", config, FluvioConfigStatus {})
                                    .with_context(ctx),
                            )
                        }
                        Err(err) => Err(K8ConvertError::KeyConvertionError(IoError::new(
                            ErrorKind::InvalidData,
                            format!("error converting metadata: {:#?}", err),
                        ))),
                    },
                    Err(err) => Err(K8ConvertError::Other(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        err.to_string(),
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
