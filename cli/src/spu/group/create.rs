//!
//! # Create Mange SPU Groups
//!
//! CLI tree to generate Create Managed SPU Groups
//!

use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;
use structopt::StructOpt;

use sc_api::spu::FlvCreateSpuGroupRequest;
use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::group_config::GroupConfig;
use super::helpers::proc_create::process_create_spu_group;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct CreateManagedSpuGroupOpt {
    /// Managed SPU group name
    #[structopt(short = "n", long = "name", value_name = "string")]
    name: String,

    /// SPU replicas
    #[structopt(short = "l", long = "replicas")]
    replicas: u16,

    /// Minimum SPU id (default: 1)
    #[structopt(short = "i", long = "min-id")]
    min_id: Option<i32>,

    /// Rack name
    #[structopt(short = "r", long = "rack", value_name = "string")]
    rack: Option<String>,

    /// storage size
    #[structopt(short = "s", long = "size", value_name = "string")]
    storage: Option<String>,



    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,


    /// Profile name
    #[structopt(short = "P", long = "profile")]
    profile: Option<String>,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------
pub fn process_create_managed_spu_group(opt: CreateManagedSpuGroupOpt) -> Result<(), CliError> {
    let (target_server, create_spu_group_cfg) = parse_opt(opt)?;

    debug!("{:#?}", create_spu_group_cfg);

    match target_server {
        TargetServer::Sc(server_addr) => {
            process_create_spu_group(server_addr, create_spu_group_cfg)
        }
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("invalid sc server {:?}", target_server),
        ))),
    }
}

/// Validate cli options. Generate target-server and create spu group config.
fn parse_opt(
    opt: CreateManagedSpuGroupOpt,
) -> Result<(TargetServer, FlvCreateSpuGroupRequest), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    let grp_config = opt.storage.map(|storage| GroupConfig::with_storage(storage));

    let group = FlvCreateSpuGroupRequest {
        name: opt.name,
        replicas: opt.replicas,
        min_id: opt.min_id,
        config: grp_config.map(|cf| cf.into()).unwrap_or_default(),
        rack: opt.rack
    };
    
    // return server separately from config

    Ok((target_server, group))
}
