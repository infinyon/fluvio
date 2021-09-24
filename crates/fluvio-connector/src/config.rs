use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;
use crate::error::ConnectorError;

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectorConfig {
    name: String,
    #[serde(rename = "type")]
    type_: String,
    pub(crate) topic: String,
    #[serde(default)]
    pub(crate) create_topic: bool,
    #[serde(default)]
    parameters: BTreeMap<String, String>,
    #[serde(default)]
    secrets: BTreeMap<String, String>,
}

impl ConnectorConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<ConnectorConfig, ConnectorError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let connector_config: ConnectorConfig = serde_yaml::from_str(&contents)?;
        Ok(connector_config)
    }
}

impl From<ConnectorConfig> for ManagedConnectorSpec {
    fn from(config: ConnectorConfig) -> ManagedConnectorSpec {
        ManagedConnectorSpec {
            name: config.name,
            type_: config.type_,
            topic: config.topic,
            parameters: config.parameters,
            secrets: config.secrets,
        }
    }
}

#[test]
fn config_test() {
    let _: ManagedConnectorSpec = ConnectorConfig::from_file("test-data/test-config.yaml")
        .expect("Failed to load test config")
        .into();
}
