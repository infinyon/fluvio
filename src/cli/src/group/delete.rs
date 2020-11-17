//!
//! # Delete Managed SPU Groups
//!
//! CLI tree to generate Delete Managed SPU Groups
//!
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::spg::SpuGroupSpec;
use crate::Result;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteManagedSpuGroupOpt {
    /// The name of the SPU Group to delete
    #[structopt(value_name = "name")]
    name: String,
}

impl DeleteManagedSpuGroupOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let mut admin = fluvio.admin().await;
        admin.delete::<SpuGroupSpec, _>(&self.name).await?;
        Ok(())
    }
}
