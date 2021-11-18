//!
//! # Create a TableFormat display specification
//!
//! CLI tree to generate Create a TableFormat spec
//!

use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;

use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct CreateTableFormatOpt {
    pub name: String,
    /// The name for the new TableFormat spec
    #[structopt(short, long, parse(from_os_str))]
    pub config: Option<PathBuf>, // PathBuf
}

impl CreateTableFormatOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let tableformat_spec = TableFormatSpec {
            name: self.name.clone(),
            ..Default::default()
        };

        debug!(
            "creating tableformat: {} spec: {:#?}",
            self.name, tableformat_spec
        );
        let admin = fluvio.admin().await;
        admin
            .create(self.name.clone(), false, tableformat_spec)
            .await?;
        println!("tableformat \"{}\" created", self.name);

        Ok(())
    }
}
