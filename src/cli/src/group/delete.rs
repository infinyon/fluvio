//!
//! # Delete Managed SPU Groups
//!
//! CLI tree to generate Delete Managed SPU Groups
//!
use structopt::StructOpt;

use flv_client::ClusterConfig;
use flv_client::metadata::spg::SpuGroupSpec;
use crate::error::CliError;
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
    fn validate(self) -> Result<(ClusterConfig, String), CliError> {
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

    let mut client = target_server.connect().await?;
    let mut admin = client.admin().await;
    admin.delete::<SpuGroupSpec, _>(&name).await?;
    Ok(())
}
