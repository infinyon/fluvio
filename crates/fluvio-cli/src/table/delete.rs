//!
//! # Delete Table spec
//!
//! CLI tree to generate Delete Table spec
//!
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::table::TableSpec;

use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteTableOpt {
    /// The name of the connector to delete
    name: String,
}

impl DeleteTableOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;
        admin.delete::<TableSpec, _>(&self.name).await?;
        Ok(())
    }
}
