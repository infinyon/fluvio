//!
//! # Delete Custom SPUs
//!
//! CLI tree to generate Delete Custom SPUs
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use flv_client::metadata::spu::CustomSpuSpec;
use flv_client::metadata::spu::CustomSpuKey;
use flv_client::ClusterConfig;

use crate::target::ClusterTarget;
use crate::error::CliError;

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
    fn validate(self) -> Result<(ClusterConfig, CustomSpuKey), CliError> {
        let target_server = self.target.load()?;

        // custom spu
        let custom_spu = if let Some(name) = self.name {
            CustomSpuKey::Name(name)
        } else if let Some(id) = self.id {
            CustomSpuKey::Id(id)
        } else {
            return Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                "missing custom SPU name or id",
            )));
        };

        // return server separately from config
        Ok((target_server, custom_spu))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process unregister custom-spu cli request
pub async fn process_unregister_custom_spu(opt: UnregisterCustomSpuOpt) -> Result<(), CliError> {
    let (target_server, delete_key) = opt.validate()?;

    let mut client = target_server.connect().await?;
    let mut admin = client.admin().await;

    admin.delete::<CustomSpuSpec, _>(delete_key).await?;
    Ok(())
}
