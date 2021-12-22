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
use crate::tableformat::TableFormatConfig;

use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default, Clone)]
pub struct CreateTableFormatOpt {
    /// The path to the TableFormat config
    #[structopt(short, long, parse(from_os_str))]
    pub config: PathBuf,
}

impl CreateTableFormatOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
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
