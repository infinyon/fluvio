use std::collections::HashMap;

use tracing::debug;

use k8_client::{ClientError, SharedK8Client};
use k8_metadata_client::MetadataClient;
use k8_types::core::pod::{ResourceRequirements, PodSecurityContext};
use k8_types::core::config_map::ConfigMapSpec;
use k8_types::InputObjectMeta;
use k8_types::core::service::ServiceSpec;

const CONFIG_MAP_NAME: &str = "spu-k8";

#[derive(Debug)]
pub struct SpuK8Config {
    pub image: String,
    pub resources: Option<ResourceRequirements>,
    pub pod_security_context: Option<PodSecurityContext>,
    pub lb_service_annotations: HashMap<String, String>,
    pub service: Option<ServiceSpec>
}

impl SpuK8Config {
    pub async fn load(client: &SharedK8Client, namespace: &str) -> Result<Self, ClientError> {
        let meta = InputObjectMeta::named(CONFIG_MAP_NAME, namespace);
        let k8_obj = client.retrieve_item::<ConfigMapSpec, _>(&meta).await?;
        let mut data = k8_obj.header.data;

        debug!("ConfigMap {} data: {:?}", CONFIG_MAP_NAME, data);

        let image = data.remove("image").ok_or_else(|| {
            ClientError::Other("image not found in ConfigMap spu-k8 data".to_owned())
        })?;

        let resources = if let Some(resources_string) = data.remove("resources") {
            serde_json::from_str(&resources_string)?
        } else {
            None
        };

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

        let service = 
            if let Some(service_data) = data.remove("service") {
                Some(serde_json::from_str(&service_data)?)
            } else {
                None
            };

        Ok(Self {
            image,
            resources,
            pod_security_context,
            lb_service_annotations,
            service
        })
    }

    /// apply service config to service
    pub fn apply_service(&self, service: &mut ServiceSpec) {
        if let Some(service_template) = &self.service {
            if let Some(ty) = &service_template.r#type {
                service.r#type = Some(ty.clone());
            }
        }
    }
}
