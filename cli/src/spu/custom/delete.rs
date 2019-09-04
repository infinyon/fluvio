//!
//! # Delete Custom SPUs
//!
//! CLI tree to generate Delete Custom SPUs
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt;

use structopt::StructOpt;

use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::proc_delete::process_sc_delete_custom_spu;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteCustomSpuOpt {
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

    /// Profile name
    #[structopt(short = "P", long = "profile")]
    profile: Option<String>,
}

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct DeleteCustomSpuConfig {
    pub custom_spu: CustomSpu,
}

#[derive(Debug)]
pub enum CustomSpu {
    Name(String),
    Id(i32),
}

impl fmt::Display for CustomSpu {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CustomSpu::Name(name) => write!(f, "{}", name),
            CustomSpu::Id(id) => write!(f, "{}", id),
        }
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete custom-spu cli request
pub fn process_delete_custom_spu(opt: DeleteCustomSpuOpt) -> Result<(), CliError> {
    let (target_server, delete_custom_spu_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Sc(server_addr) => {
            process_sc_delete_custom_spu(server_addr, delete_custom_spu_cfg)
        }
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("invalid sc server {:?}", target_server),
        ))),
    }
}
/// Validate cli options. Generate target-server and delete custom spu config.
fn parse_opt(opt: DeleteCustomSpuOpt) -> Result<(TargetServer, DeleteCustomSpuConfig), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // custom spu
    let custom_spu = if let Some(name) = opt.name {
        CustomSpu::Name(name)
    } else if let Some(id) = opt.id {
        CustomSpu::Id(id)
    } else {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "missing custom SPU name or id",
        )));
    };

    // delete custom spu config
    let cfg = DeleteCustomSpuConfig { custom_spu };

    // return server separately from config
    Ok((target_server, cfg))
}
