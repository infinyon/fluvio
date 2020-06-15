//!
//! # Delete Custom SPUs
//!
//! CLI tree to generate Delete Custom SPUs
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use sc_api::server::spu::FlvCustomSpu;
use flv_client::profile::ScConfig;

use crate::error::CliError;
use crate::tls::TlsConfig;
use crate::profile::InlineProfile;

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

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    #[structopt(flatten)]
    tls: TlsConfig,

    #[structopt(flatten)]
    profile: InlineProfile,
}

impl UnregisterCustomSpuOpt {
    /// Validate cli options. Generate target-server and unregister custom spu config.
    fn validate(self) -> Result<(ScConfig, FlvCustomSpu), CliError> {
        let target_server = ScConfig::new_with_profile(
            self.sc,
            self.tls.try_into_file_config()?,
            self.profile.profile,
        )?;

        // custom spu
        let custom_spu = if let Some(name) = self.name {
            FlvCustomSpu::Name(name)
        } else if let Some(id) = self.id {
            FlvCustomSpu::Id(id)
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
    let (target_server, cfg) = opt.validate()?;

    let mut sc = target_server.connect().await?;

    sc.unregister_custom_spu(cfg)
        .await
        .map_err(|err| err.into())
}
