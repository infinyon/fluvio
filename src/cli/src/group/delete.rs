//!
//! # Delete Managed SPU Groups
//!
//! CLI tree to generate Delete Managed SPU Groups
//!
use structopt::StructOpt;

use fluvio::{Fluvio, FluvioConfig};
use fluvio::metadata::spg::SpuGroupSpec;
use crate::target::ClusterTarget;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteManagedSpuGroupOpt {
    /// Managed SPU group name
    #[structopt(short = "n", long = "name", value_name = "string")]
    name: String,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl DeleteManagedSpuGroupOpt {
    /// Validate cli options. Generate target-server and delete spu group configuration.
    fn validate(self) -> eyre::Result<(FluvioConfig, String)> {
        let target_server = self.target.load()?;

        Ok((target_server, self.name))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete custom-spu cli request
pub async fn process_delete_managed_spu_group(opt: DeleteManagedSpuGroupOpt) -> eyre::Result<()> {
    let (target_server, name) = opt.validate()?;

    let mut client = Fluvio::connect_with_config(&target_server).await?;
    let mut admin = client.admin().await;
    admin.delete::<SpuGroupSpec, _>(&name).await?;
    Ok(())
}
