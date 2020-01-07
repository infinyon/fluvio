//!
//! # Delete Managed SPU Groups
//!
//! CLI tree to generate Delete Managed SPU Groups
//!
use structopt::StructOpt;

use crate::error::CliError;
use flv_client::profile::ScConfig;

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct DeleteManagedSpuGroupConfig {
    pub name: String,
}

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteManagedSpuGroupOpt {
    /// Managed SPU group name
    #[structopt(short = "n", long = "name", value_name = "string")]
    name: String,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    /// Profile name
    #[structopt(short = "P", long = "profile")]
    profile: Option<String>,
}

impl DeleteManagedSpuGroupOpt {
    /// Validate cli options. Generate target-server and delete spu group configuration.
    fn validate(self) -> Result<(ScConfig, DeleteManagedSpuGroupConfig), CliError> {
        let target_server = ScConfig::new(self.sc, self.profile)?;

        let delete_spu_group_cfg = DeleteManagedSpuGroupConfig { name: self.name };

        // return server separately from config
        Ok((target_server, delete_spu_group_cfg))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete custom-spu cli request
pub async fn process_delete_managed_spu_group(
    opt: DeleteManagedSpuGroupOpt,
) -> Result<(), CliError> {
    let (target_server, delete_spu_group_cfg) = opt.validate()?;

    let mut sc = target_server.connect().await?;

    sc.delete_group(&delete_spu_group_cfg.name)
        .await
        .map_err(|err| err.into())
}
