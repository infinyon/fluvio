//!
//! # ConfigMap Key/Value Store Actions
//!
//! Actions for ConfigMap communication with Key Value store.
//!
use std::collections::BTreeMap;

use log::{debug, trace};

use flv_metadata::k8::metadata::*;
use flv_metadata::k8::core::config_map::ConfigMapSpec;
use k8_client::metadata::MetadataClient;
use k8_client::SharedK8Client;

use crate::ScK8Error;

#[allow(dead_code)]
/// Establish connection to K8 and create a new config_map
pub async fn add_config_map(
    client: SharedK8Client,
    config_map_name: String,
    data: BTreeMap<String, String>,
) -> Result<(), ScK8Error> {
    debug!(
        "apply config_map '{}' with {} entries",
        config_map_name,
        data.len()
    );

    let new_map: InputK8Obj<ConfigMapSpec> = InputK8Obj {
        api_version: ConfigMapSpec::api_version(),
        kind: ConfigMapSpec::kind(),
        metadata: InputObjectMeta {
            name: config_map_name,
            namespace: "default".to_string(),
            ..Default::default()
        },
        data,
        ..Default::default()
    };

    trace!("send create config_map to K8 {:#?}", &new_map);

    client.apply::<ConfigMapSpec>(new_map).await?;

    Ok(())
}
