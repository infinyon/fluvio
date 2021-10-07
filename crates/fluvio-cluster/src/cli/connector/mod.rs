use std::sync::Arc;
use structopt::StructOpt;

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;
use fluvio_extension_common::Terminal;
use fluvio_extension_common::COMMAND_TEMPLATE;

mod create;
mod delete;
mod list;

use create::CreateManagedConnectorOpt;
use delete::DeleteManagedConnectorOpt;
use list::ListManagedConnectorsOpt;
use crate::cli::ClusterCliError;

#[derive(Debug, StructOpt)]
pub enum ManagedConnectorCmd {
    /// Create a new Managed Connector
    #[structopt(
        name = "create",
        template = COMMAND_TEMPLATE,
    )]
    Create(CreateManagedConnectorOpt),

    /// Delete a Managed Connector
    #[structopt(
        name = "delete",
        template = COMMAND_TEMPLATE,
    )]
    Delete(DeleteManagedConnectorOpt),

    /// List all Managed Connectors
    #[structopt(
        name = "list",
        template = COMMAND_TEMPLATE,
    )]
    List(ListManagedConnectorsOpt),
}

impl ManagedConnectorCmd {
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<(), ClusterCliError> {
        match self {
            Self::Create(create) => {
                create.process(fluvio).await?;
            }
            Self::Delete(delete) => {
                delete.process(fluvio).await?;
            }
            Self::List(list) => {
                list.process(out, fluvio).await?;
            }
        }
        Ok(())
    }
}

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
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<ConnectorConfig, ClusterCliError> {
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
