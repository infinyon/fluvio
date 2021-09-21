use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;
use crate::error::ConnectorError;

#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    name: String,
    #[serde(rename = "type")]
    type_: String,
    topic: String,
    create_topic: Option<bool>,
    #[serde(default = "ConnectorConfig::default_args")]
    parameters: BTreeMap<String, String>,
    #[serde(default = "ConnectorConfig::default_args")]
    secrets: BTreeMap<String, String>,
}

pub type ConnectorConfigSet = ConnectorConfig;

impl ConnectorConfig {
    fn default_args() -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<ConnectorConfigSet, ConnectorError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let connector_config: ConnectorConfigSet = serde_yaml::from_str(&contents)?;
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
