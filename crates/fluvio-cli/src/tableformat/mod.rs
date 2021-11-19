use std::sync::Arc;
use structopt::StructOpt;

use serde::Deserialize;
use std::path::PathBuf;
use std::fs::File;
use std::io::Read;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::tableformat::{InputFormat, TableFormatSpec, TableFormatColumnConfig};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::COMMAND_TEMPLATE;
mod create;
mod delete;
mod list;

use create::CreateTableFormatOpt;
use delete::DeleteTableFormatOpt;
use list::ListTableFormatsOpt;
use crate::CliError;

#[derive(Debug, StructOpt)]
pub enum TableFormatCmd {
    /// Create a new TableFormat display
    #[structopt(
        name = "create",
        template = COMMAND_TEMPLATE,
    )]
    Create(CreateTableFormatOpt),

    /// Delete a TableFormat display
    #[structopt(
        name = "delete",
        template = COMMAND_TEMPLATE,
    )]
    Delete(DeleteTableFormatOpt),

    /// List all TableFormat display
    #[structopt(
        name = "list",
        template = COMMAND_TEMPLATE,
    )]
    List(ListTableFormatsOpt),
}

impl TableFormatCmd {
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

#[derive(Debug, Deserialize, Clone)]
pub struct TableFormatConfig {
    pub name: String,
    pub input_format: Option<InputFormat>,
    pub columns: Vec<TableFormatColumnConfig>,
    pub smartmodule: Option<String>,
}

impl TableFormatConfig {
    pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<TableFormatConfig, CliError> {
        let mut file = File::open(path.into())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let table_format_config: TableFormatConfig = serde_yaml::from_str(&contents)?;
        Ok(table_format_config)
    }
}

impl From<TableFormatConfig> for TableFormatSpec {
    fn from(config: TableFormatConfig) -> TableFormatSpec {
        TableFormatSpec {
            name: config.name,
            input_format: config.input_format.unwrap_or_default(),
            columns: config.columns,
            smartmodule: config.smartmodule,
        }
    }
}

#[test]
fn config_test() {
    let _: TableFormatSpec = TableFormatConfig::from_file("test-data/test-tableformat-config.yaml")
        .expect("Failed to load test config")
        .into();
}
