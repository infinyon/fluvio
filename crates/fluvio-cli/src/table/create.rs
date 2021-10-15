//!
//! # Create a Table display specification
//!
//! CLI tree to generate Create a Table spec
//!

use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::table::TableSpec;

use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt, Default)]
pub struct CreateTableOpt {
    pub name: String,
    /// The name for the new Table spec
    #[structopt(short, long, parse(from_os_str))]
    pub config: Option<PathBuf>, // PathBuf
}

impl CreateTableOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let table_spec = TableSpec::default();

        debug!("creating table: {} spec: {:#?}", self.name, table_spec);
        let admin = fluvio.admin().await;
        admin.create(self.name.clone(), false, table_spec).await?;
        println!("table \"{}\" created", self.name);

        Ok(())
    }
}
