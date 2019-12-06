//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::process_sc_delete_topic;
use super::helpers::process_kf_delete_topic;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteTopicOpt {
    /// Topic name
    #[structopt(short = "t", long = "topic", value_name = "string")]
    topic: String,

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

    /// Profile name
    #[structopt(short = "P", long = "profile")]
    profile: Option<String>,
}

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct DeleteTopicConfig {
    pub name: String,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete topic cli request
pub fn process_delete_topic(opt: DeleteTopicOpt) -> Result<(), CliError> {
    let (target_server, delete_topic_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Kf(server_addr) => process_kf_delete_topic(server_addr, delete_topic_cfg),
        TargetServer::Sc(server_addr) => process_sc_delete_topic(server_addr, delete_topic_cfg),
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("Invalid Target Server Server {:?}", target_server),
        ))),
    }
}

/// Validate cli options. Generate target-server and delete-topic configuration.
fn parse_opt(opt: DeleteTopicOpt) -> Result<(TargetServer, DeleteTopicConfig), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new(&opt.sc, &opt.kf, &opt.profile)?;
    let target_server = profile_config.target_server()?;
    let delete_topic_cfg = DeleteTopicConfig { name: opt.topic };

    // return server separately from config
    Ok((target_server, delete_topic_cfg))
}
