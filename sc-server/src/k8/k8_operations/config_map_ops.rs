//!
//! # ConfigMap Key/Value Store Actions
//!
//! Actions for ConfigMap communication with Key Value store.
//!
use std::collections::BTreeMap;

use log::{debug, trace};

use k8_metadata::core::metadata::InputK8Obj;
use k8_metadata::core::metadata::InputObjectMeta;
use k8_metadata::core::Spec;
use k8_client::config_map::ConfigMapSpec;


use crate::k8::SharedK8Client;
use crate::ScServerError;

#[allow(dead_code)]
/// Establish connection to K8 and create a new config_map
pub async fn add_config_map(
    client: SharedK8Client,
    config_map_name: String,
    data: BTreeMap<String, String>,
) -> Result<(), ScServerError> {
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

    client
        .apply::<ConfigMapSpec>(new_map)
        .await?;
    

    Ok(())
}
