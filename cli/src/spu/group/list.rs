//! # List SPU Groups CLI
//!
//! CLI tree and processing to list SPU Groups
//!
use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use crate::error::CliError;
use crate::common::OutputType;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::proc_list::query_spu_group_metadata;
use super::helpers::list_output::spu_group_response_to_output;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct ListManagedSpuGroupsOpt {
    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    ///Profile name
    #[structopt(short = "P", long = "profile")]
    pub profile: Option<String>,

    /// Output
    #[structopt(
        short = "O",
        long = "output",
        value_name = "type",
        raw(possible_values = "&OutputType::variants()", case_insensitive = "true")
    )]
    output: Option<OutputType>,
}

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct ListSpuGroupsConfig {
    pub output: OutputType,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process list spus cli request
pub fn process_list_managed_spu_groups(opt: ListManagedSpuGroupsOpt) -> Result<(), CliError> {
    let (target_server, list_spu_group_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Sc(server_addr) => {
            fetch_and_list_spu_groups(server_addr, &list_spu_group_cfg)
        }
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "Invalid SC server configuration",
        ))),
    }
}

/// Validate cli options and generate config
fn parse_opt(
    opt: ListManagedSpuGroupsOpt,
) -> Result<(TargetServer, ListSpuGroupsConfig), CliError> {
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // transfer config parameters
    let list_spu_group_cfg = ListSpuGroupsConfig {
        output: opt.output.unwrap_or(OutputType::default()),
    };

    // return server separately from topic result
    Ok((target_server, list_spu_group_cfg))
}

// Fetch Spu groups and output in desired format
fn fetch_and_list_spu_groups(
    server_addr: SocketAddr,
    list_spu_group_cfg: &ListSpuGroupsConfig,
) -> Result<(), CliError> {
    let flv_spu_groups = query_spu_group_metadata(server_addr)?;
    spu_group_response_to_output(flv_spu_groups, &list_spu_group_cfg.output)
}
