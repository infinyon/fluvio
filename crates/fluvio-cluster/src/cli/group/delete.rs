//!
//! # Delete Managed SPU Groups
//!
//! CLI tree to generate Delete Managed SPU Groups
//!
use clap::Parser;

use fluvio::Fluvio;
use fluvio::metadata::spg::SpuGroupSpec;

use crate::cli::ClusterCliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser)]
pub struct DeleteManagedSpuGroupOpt {
    /// The name of the SPU Group to delete
    #[clap(value_name = "name")]
    name: String,
}

impl DeleteManagedSpuGroupOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), ClusterCliError> {
        let admin = fluvio.admin().await;
        admin.delete::<SpuGroupSpec, _>(&self.name).await?;
        Ok(())
    }
}
