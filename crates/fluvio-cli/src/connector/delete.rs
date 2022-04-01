//!
//! # Delete Managed Connectors
//!
//! CLI tree to generate Delete Managed Connectors
//!
use clap::Parser;

use fluvio::Fluvio;
use fluvio::metadata::connector::ManagedConnectorSpec;

use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser)]
pub struct DeleteManagedConnectorOpt {
    /// The name of the connector to delete
    #[clap(value_name = "name")]
    name: String,
}

impl DeleteManagedConnectorOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;
        admin.delete::<ManagedConnectorSpec, _>(&self.name).await?;
        Ok(())
    }
}
