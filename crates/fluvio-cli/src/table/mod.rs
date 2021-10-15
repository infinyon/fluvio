use std::sync::Arc;
use structopt::StructOpt;

//use serde::Deserialize;
//use std::collections::BTreeMap;
//use std::path::PathBuf;
//use std::fs::File;
//use std::io::Read;

use fluvio::Fluvio;
//use fluvio_controlplane_metadata::table::TableSpec;
use fluvio_extension_common::Terminal;
use fluvio_extension_common::COMMAND_TEMPLATE;

mod create;
mod delete;
mod list;

use create::CreateTableOpt;
use delete::DeleteTableOpt;
use list::ListTablesOpt;
use crate::CliError;

#[derive(Debug, StructOpt)]
pub enum TableCmd {
    /// Create a new Table display
    #[structopt(
        name = "create",
        template = COMMAND_TEMPLATE,
    )]
    Create(CreateTableOpt),

    /// Delete a Table display
    #[structopt(
        name = "delete",
        template = COMMAND_TEMPLATE,
    )]
    Delete(DeleteTableOpt),

    /// List all Table display
    #[structopt(
        name = "list",
        template = COMMAND_TEMPLATE,
    )]
    List(ListTablesOpt),
}

impl TableCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<(), CliError> {
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

//#[derive(Debug, Deserialize, Clone)]
//pub struct ConnectorConfig {
//    name: String,
//    #[serde(rename = "type")]
//    type_: String,
//    pub(crate) topic: String,
//    #[serde(default)]
//    pub(crate) create_topic: bool,
//    #[serde(default)]
//    parameters: BTreeMap<String, String>,
//    #[serde(default)]
//    secrets: BTreeMap<String, String>,
//}
//
//impl ConnectorConfig {
//    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<ConnectorConfig, CliError> {
//        let mut file = File::open(path.into())?;
//        let mut contents = String::new();
//        file.read_to_string(&mut contents)?;
//        let connector_config: ConnectorConfig = serde_yaml::from_str(&contents)?;
//        Ok(connector_config)
//    }
//}
//
//impl From<ConnectorConfig> for TableSpec {
//    fn from(config: ConnectorConfig) -> TableSpec {
//        TableSpec {
//            name: config.name,
//            type_: config.type_,
//            topic: config.topic,
//            parameters: config.parameters,
//            secrets: config.secrets,
//        }
//    }
//}

//#[test]
//fn config_test() {
//    let _: TableSpec = ConnectorConfig::from_file("test-data/test-config.yaml")
//        .expect("Failed to load test config")
//        .into();
//}
