//!
//! # Delete Custom SPUs
//!
//! CLI tree to generate Delete Custom SPUs
//!
use structopt::StructOpt;
use anyhow::anyhow;

use fluvio::metadata::spu::CustomSpuSpec;
use fluvio::metadata::spu::CustomSpuKey;
use fluvio::{Fluvio, FluvioConfig};

use crate::target::ClusterTarget;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct UnregisterCustomSpuOpt {
    /// SPU id
    #[structopt(short = "i", long = "id", required_unless = "name")]
    id: Option<i32>,

    /// SPU name
    #[structopt(
        short = "n",
        long = "name",
        value_name = "string",
        conflicts_with = "id"
    )]
    name: Option<String>,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl UnregisterCustomSpuOpt {
    /// Validate cli options. Generate target-server and unregister custom spu config.
    fn validate(self) -> anyhow::Result<(FluvioConfig, CustomSpuKey)> {
        let target_server = self.target.load()?;

        // custom spu
        let custom_spu = if let Some(name) = self.name {
            CustomSpuKey::Name(name)
        } else if let Some(id) = self.id {
            CustomSpuKey::Id(id)
        } else {
            return Err(anyhow!("Missing custom SPU name or id"));
        };

        // return server separately from config
        Ok((target_server, custom_spu))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process unregister custom-spu cli request
pub async fn process_unregister_custom_spu(opt: UnregisterCustomSpuOpt) -> anyhow::Result<()> {
    let (target_server, delete_key) = opt.validate()?;

    let mut client = Fluvio::connect_with_config(&target_server).await?;
    let mut admin = client.admin().await;

    admin.delete::<CustomSpuSpec, _>(delete_key).await?;
    Ok(())
}
