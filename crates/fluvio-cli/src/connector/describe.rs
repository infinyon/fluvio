//! # List Managed Connectors CLI
//!
//! CLI tree and processing to list Managed Connectors
//!

use std::sync::Arc;
use std::path::PathBuf;
use std::fs::File;
use std::io::Write;
use clap::Parser;

use fluvio::Fluvio;
use fluvio::metadata::connector::ManagedConnectorSpec;

use fluvio_extension_common::Terminal;
use crate::CliError;
use super::ConnectorConfig;

#[derive(Debug, Parser)]
pub struct DescribeManagedConnectorOpt {
    #[clap(name = "connector-name")]
    pub connector_name: String,

    #[clap(short, long, name = "out-file")]
    out_file: Option<PathBuf>,
}
use fluvio_extension_common::t_println;

impl DescribeManagedConnectorOpt {
    /// Process list connectors cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;
        let connector_name = self.connector_name;
        let connector_list = admin.list::<ManagedConnectorSpec, _>(vec![connector_name.clone()]).await?;
        let connector = connector_list.first();
        let connector = if let Some(connector) = connector {
            connector.spec.clone()
        } else {
            return Err(CliError::ConnectorNotFound(connector_name));
        };
        let connector : ConnectorConfig = connector.into();
        let connector  = serde_yaml::to_string(&connector)?;

        if let Some(path) = self.out_file {
            let mut file = File::create(path)?;
            write!(file, "{}", connector)?;

        } else {
            t_println!(out, "{}", connector);
        }

        Ok(())

    }
}
