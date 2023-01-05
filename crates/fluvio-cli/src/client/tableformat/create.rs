//!
//! # Create a TableFormat display specification
//!
//! CLI tree to generate Create a TableFormat spec
//!

use std::path::PathBuf;

use clap::Parser;
use tracing::debug;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::tableformat::TableFormatSpec;

use super::TableFormatConfig;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser, Default)]
pub struct CreateTableFormatOpt {
    /// The path to the TableFormat config
    #[clap(short, long, value_parser)]
    pub config: PathBuf,
}

impl CreateTableFormatOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let config = TableFormatConfig::from_file(self.config)?;
        let tableformat_spec: TableFormatSpec = config.into();
        let name = tableformat_spec.name.clone();

        debug!(
            "creating tableformat: {} spec: {:#?}",
            &name, tableformat_spec
        );

        let admin = fluvio.admin().await;
        admin.create(name.clone(), false, tableformat_spec).await?;
        println!("tableformat \"{}\" created", &name);

        Ok(())
    }
}
