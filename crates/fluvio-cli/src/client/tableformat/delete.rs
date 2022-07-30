//!
//! # Delete TableFormat spec
//!
//! CLI tree to generate Delete TableFormat spec
//!
use clap::Parser;

use fluvio::Fluvio;
use fluvio::metadata::tableformat::TableFormatSpec;

use crate::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser)]
pub struct DeleteTableFormatOpt {
    /// The name of the connector to delete
    name: String,
}

impl DeleteTableFormatOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;
        admin.delete::<TableFormatSpec, _>(&self.name).await?;
        Ok(())
    }
}
