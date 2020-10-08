//!
//! # Delete Managed SPU Groups
//!
//! CLI tree to generate Delete Managed SPU Groups
//!
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::spg::SpuGroupSpec;
use crate::error::CliError;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteManagedSpuGroupOpt {
    /// Managed SPU group name
    #[structopt(short = "n", long = "name", value_name = "string")]
    name: String,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete custom-spu cli request
pub async fn process_delete_managed_spu_group(
    fluvio: &Fluvio,
    opt: DeleteManagedSpuGroupOpt,
) -> Result<(), CliError> {
    let mut admin = fluvio.admin().await;
    admin.delete::<SpuGroupSpec, _>(&opt.name).await?;
    Ok(())
}
