//!
//! # Delete Managed SPU Groups
//!
//! CLI tree to generate Delete Managed SPU Groups
//!
use structopt::StructOpt;

use fluvio::{Fluvio, FluvioConfig};
use fluvio::metadata::spg::SpuGroupSpec;
use crate::error::CliError;
use crate::target::ClusterTarget;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteManagedSpuGroupOpt {
    /// The name of the SPU Group to delete
    #[structopt(value_name = "name")]
    name: String,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl DeleteManagedSpuGroupOpt {
    /// Validate cli options. Generate target-server and delete spu group configuration.
    fn validate(self) -> Result<(FluvioConfig, String), CliError> {
        let target_server = self.target.load()?;

        Ok((target_server, self.name))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete custom-spu cli request
pub async fn process_delete_managed_spu_group(
    opt: DeleteManagedSpuGroupOpt,
) -> Result<(), CliError> {
    let (target_server, name) = opt.validate()?;

    let client = Fluvio::connect_with_config(&target_server).await?;
    let mut admin = client.admin().await;
    admin.delete::<SpuGroupSpec, _>(&name).await?;
    Ok(())
}
