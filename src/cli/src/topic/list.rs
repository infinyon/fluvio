//!
//! # List Topics CLI
//!
//! CLI tree and processing to list Topics
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use crate::error::CliError;
use crate::common::OutputType;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::process_sc_list_topics;
use super::helpers::process_kf_list_topics;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct ListTopicsOpt {
    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    /// Address of Kafka Controller
    #[structopt(
        short = "k",
        long = "kf",
        value_name = "host:port",
        conflicts_with = "sc"
    )]
    kf: Option<String>,

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
pub struct ListTopicsConfig {
    pub output: OutputType,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process list topics cli request
pub fn process_list_topics(opt: ListTopicsOpt) -> Result<(), CliError> {
    let (target_server, list_topic_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Kf(server_addr) => process_kf_list_topics(server_addr, &list_topic_cfg),
        TargetServer::Sc(server_addr) => process_sc_list_topics(server_addr, &list_topic_cfg),
        TargetServer::Spu(_) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "SPU does not implement list topics",
        ))),
    }
}

/// Validate cli options and generate config
fn parse_opt(opt: ListTopicsOpt) -> Result<(TargetServer, ListTopicsConfig), CliError> {
    let profile_config = ProfileConfig::new(&opt.sc, &opt.kf, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // transfer config parameters
    let list_topics_cfg = ListTopicsConfig {
        output: opt.output.unwrap_or(OutputType::default()),
    };

    // return server separately from topic result
    Ok((target_server, list_topics_cfg))
}
