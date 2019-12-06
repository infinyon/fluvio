//!
//! # Delete Managed SPU Groups
//!
//! CLI tree to generate Delete Managed SPU Groups
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::proc_delete::process_delete_spu_group;

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

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct DeleteManagedSpuGroupConfig {
    pub name: String,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete custom-spu cli request
pub fn process_delete_managed_spu_group(opt: DeleteManagedSpuGroupOpt) -> Result<(), CliError> {
    let (target_server, delete_spu_group_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Sc(server_addr) => {
            process_delete_spu_group(server_addr, delete_spu_group_cfg)
        }
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("invalid sc server {:?}", target_server),
        ))),
    }
}

/// Validate cli options. Generate target-server and delete spu group configuration.
fn parse_opt(
    opt: DeleteManagedSpuGroupOpt,
) -> Result<(TargetServer, DeleteManagedSpuGroupConfig), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;
    let delete_spu_group_cfg = DeleteManagedSpuGroupConfig { name: opt.name };

    // return server separately from config
    Ok((target_server, delete_spu_group_cfg))
}
