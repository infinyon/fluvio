//!
//! # List All Spus CLI
//!
//! CLI tree and processing to list SPUs
//!
use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use crate::error::CliError;
use crate::common::OutputType;
use crate::profile::{ProfileConfig, TargetServer};

use crate::spu::helpers::query_spu_list_metadata;
use crate::spu::helpers::format_spu_response_output;
use crate::spu::helpers::flv_response_to_spu_metadata;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct ListSpusOpt {
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
pub struct ListSpusConfig {
    pub output: OutputType,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process list spus cli request
pub fn process_list_spus(opt: ListSpusOpt) -> Result<(), CliError> {
    let (target_server, list_spu_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Sc(server_addr) => fetch_and_list_all_spus(server_addr, &list_spu_cfg),
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "SC is solely responsible for SPUs",
        ))),
    }
}

/// Validate cli options and generate config
fn parse_opt(opt: ListSpusOpt) -> Result<(TargetServer, ListSpusConfig), CliError> {
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // transfer config parameters
    let list_spu_cfg = ListSpusConfig {
        output: opt.output.unwrap_or(OutputType::default()),
    };

    // return server separately from topic result
    Ok((target_server, list_spu_cfg))
}

// Retrieve all spus and output in desired format
fn fetch_and_list_all_spus(
    server_addr: SocketAddr,
    list_spu_cfg: &ListSpusConfig,
) -> Result<(), CliError> {
    let flv_spus = query_spu_list_metadata(server_addr, false)?;
    let sc_spus = flv_response_to_spu_metadata(&flv_spus);

    // format and dump to screen
    format_spu_response_output(sc_spus, &list_spu_cfg.output)
}
