mod create;
mod delete;
mod list;

pub use cmd::{TableFormatConfig, TableFormatCmd};

mod cmd {

    use std::sync::Arc;
    use std::path::PathBuf;
    use std::fs::File;
    use std::io::Read;
    use std::fmt::Debug;

    use async_trait::async_trait;
    use serde::Deserialize;
    use clap::Parser;
    use anyhow::Result;

    use fluvio::Fluvio;
    use fluvio::metadata::tableformat::{DataFormat, TableFormatSpec, TableFormatColumnConfig};
    use fluvio_extension_common::Terminal;
    use fluvio_extension_common::COMMAND_TEMPLATE;

    use crate::client::cmd::ClientCmd;

    use super::create::CreateTableFormatOpt;
    use super::delete::DeleteTableFormatOpt;
    use super::list::ListTableFormatsOpt;

    #[derive(Debug, Parser)]
    pub enum TableFormatCmd {
        /// Create a new TableFormat display
        #[clap(
            name = "create",
            help_template = COMMAND_TEMPLATE,
        )]
        Create(CreateTableFormatOpt),

        /// Delete a TableFormat display
        #[clap(
            name = "delete",
            help_template = COMMAND_TEMPLATE,
        )]
        Delete(DeleteTableFormatOpt),

        /// List all TableFormat display
        #[clap(
            name = "list",
            help_template = COMMAND_TEMPLATE,
        )]
        List(ListTableFormatsOpt),
    }

    #[async_trait]
    impl ClientCmd for TableFormatCmd {
        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            out: Arc<O>,
            fluvio: &Fluvio,
        ) -> Result<()> {
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
        pub input_format: Option<DataFormat>,
        pub columns: Option<Vec<TableFormatColumnConfig>>,
        pub smartmodule: Option<String>,
    }

    impl TableFormatConfig {
        pub fn from_file<P: Into<PathBuf>>(path: P) -> Result<TableFormatConfig> {
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
                input_format: config.input_format,
                columns: config.columns,
                smartmodule: config.smartmodule,
            }
        }
    }

    #[test]
    fn config_test() {
        let _: TableFormatSpec =
            TableFormatConfig::from_file("test-data/test-tableformat-config.yaml")
                .expect("Failed to load test config")
                .into();
    }
}
