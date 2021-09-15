use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;
use crate::error::ConnectorError;

#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    #[serde(default = "String::new")]
    name: String,
    #[serde(rename = "type")]
    type_: String,
    topic: Option<String>,
    create_topic: Option<bool>,
    #[serde(default = "ConnectorConfig::default_args")]
    args: BTreeMap<String, String>,
}

pub type ConnectorConfigSet = BTreeMap<String, ConnectorConfig>;

impl ConnectorConfig {
    fn default_args() -> BTreeMap<String, String> {
        BTreeMap::new()
    }
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<ConnectorConfigSet, ConnectorError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let mut connector_configs: ConnectorConfigSet = serde_yaml::from_str(&contents)?;
        for (key, value) in connector_configs.iter_mut() {
            value.name = key.to_string();
        }
        Ok(connector_configs)
    }
}
impl From<ConnectorConfig> for ManagedConnectorSpec {
    fn from(config: ConnectorConfig) -> ManagedConnectorSpec {
        let topic = config.topic.unwrap_or(config.type_.clone());

        ManagedConnectorSpec {
            name: config.name,
            type_: config.type_,
            topic: topic.to_string(),
            args: config.args,
        }
    }
}
