use k8_client::{ClientError, SharedK8Client};
use k8_metadata_client::MetadataClient;
use tracing::debug;

use crate::dispatcher::k8::core::pod::ResourceRequirements;
use crate::dispatcher::k8::core::config_map::ConfigMapSpec;
use crate::dispatcher::k8::metadata::InputObjectMeta;

const CONFIG_MAP_NAME: &str = "spu-k8";

#[derive(Debug)]
pub struct SpuK8Config {
    pub image: String,
    pub resources: ResourceRequirements,
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

        let resources_string = data.remove("resources").ok_or_else(|| {
            ClientError::Other("resources not found in ConfigMap spu-k8 data".to_owned())
        })?;

        let resources = serde_json::from_str(&resources_string)?;

        Ok(Self { image, resources })
    }
}
