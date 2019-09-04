//!
//! # Create Custom SPUs
//!
//! CLI tree to generate Create Custom SPUs
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryFrom;

use structopt::StructOpt;
use types::socket_helpers::ServerAddress;

use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::proc_create::process_sc_create_custom_spu;

// -----------------------------------
// CLI Options
// -----------------------------------

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

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct CreateCustomSpuConfig {
    pub id: i32,
    pub name: String,
    pub public_server: ServerAddress,
    pub private_server: ServerAddress,
    pub rack: Option<String>,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------
pub fn process_create_custom_spu(opt: CreateCustomSpuOpt) -> Result<(), CliError> {
    let (target_server, create_custom_spu_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Sc(server_addr) => {
            process_sc_create_custom_spu(server_addr, create_custom_spu_cfg)
        }
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("invalid sc server {:?}", target_server),
        ))),
    }
}

/// Validate cli options. Generate target-server and create custom spu config.
fn parse_opt(opt: CreateCustomSpuOpt) -> Result<(TargetServer, CreateCustomSpuConfig), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // create custom spu config
    let cfg = CreateCustomSpuConfig {
        id: opt.id,
        name: opt.name.unwrap_or(format!("custom-spu-{}", opt.id)),
        public_server: TryFrom::try_from(&opt.public_server)?,
        private_server: TryFrom::try_from(&opt.private_server)?,
        rack: opt.rack.clone(),
    };

    // return server separately from config
    Ok((target_server, cfg))
}
