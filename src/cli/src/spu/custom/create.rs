//!
//! # Create Custom SPUs
//!
//! CLI tree to generate Create Custom SPUs
//!

use std::convert::TryFrom;

use structopt::StructOpt;

use types::socket_helpers::ServerAddress;

use crate::error::CliError;
use flv_client::profile::ScConfig;

#[derive(Debug)]
pub struct CreateCustomSpuConfig {
    pub id: i32,
    pub name: String,
    pub public_server: ServerAddress,
    pub private_server: ServerAddress,
    pub rack: Option<String>,
}

#[derive(Debug, StructOpt)]
pub struct CreateCustomSpuOpt {
    /// SPU id
    #[structopt(short = "i", long = "id")]
    id: i32,

    /// SPU name
    #[structopt(short = "n", long = "name", value_name = "string")]
    name: Option<String>,

    /// Rack name
    #[structopt(short = "r", long = "rack", value_name = "string")]
    rack: Option<String>,

    /// Public server::port
    #[structopt(short = "p", long = "public-server", value_name = "host:port")]
    public_server: String,

    /// Private server::port
    #[structopt(short = "v", long = "private-server", value_name = "host:port")]
    private_server: String,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    /// Profile name
    #[structopt(short = "P", long = "profile")]
    profile: Option<String>,
}

impl CreateCustomSpuOpt {
    /// Validate cli options. Generate target-server and create custom spu config.
    fn validate(self) -> Result<(ScConfig, CreateCustomSpuConfig), CliError> {
        // profile specific configurations (target server)
        let target_server = ScConfig::new(self.sc, self.profile)?;

        // create custom spu config
        let cfg = CreateCustomSpuConfig {
            id: self.id,
            name: self.name.unwrap_or(format!("custom-spu-{}", self.id)),
            public_server: TryFrom::try_from(self.public_server)?,
            private_server: TryFrom::try_from(self.private_server)?,
            rack: self.rack.clone(),
        };

        // return server separately from config
        Ok((target_server, cfg))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------
pub async fn process_create_custom_spu(opt: CreateCustomSpuOpt) -> Result<(), CliError> {
    let (target_server, cfg) = opt.validate()?;

    let mut sc = target_server.connect().await?;

    sc.create_custom_spu(
        cfg.id,
        cfg.name,
        cfg.public_server,
        cfg.private_server,
        cfg.rack,
    )
    .await
    .map_err(|err| err.into())
}
