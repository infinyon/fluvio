//!
//! # Describe Topic CLI
//!
//! CLI to describe Topics and their corresponding Partitions
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use crate::error::CliError;
use crate::common::OutputType;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::process_sc_describe_topics;
use super::helpers::process_kf_describe_topics;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DescribeTopicsOpt {
    /// Topic names
    #[structopt(short = "t", long = "topic", value_name = "string")]
    topics: Vec<String>,

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
pub struct DescribeTopicsConfig {
    pub topic_names: Vec<String>,
    pub output: OutputType,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process describe topic cli request
pub fn process_describe_topics(opt: DescribeTopicsOpt) -> Result<(), CliError> {
    let (target_server, describe_topics_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Kf(server_addr) => {
            process_kf_describe_topics(server_addr, &describe_topics_cfg)
        }
        TargetServer::Sc(server_addr) => {
            process_sc_describe_topics(server_addr, &describe_topics_cfg)
        }
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("invalid target server {:?}", target_server),
        ))),
    }
}

/// Validate cli options and generate config
fn parse_opt(opt: DescribeTopicsOpt) -> Result<(TargetServer, DescribeTopicsConfig), CliError> {
    let profile_config = ProfileConfig::new(&opt.sc, &opt.kf, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // transfer config parameters
    let describe_topics_cfg = DescribeTopicsConfig {
        output: opt.output.unwrap_or(OutputType::default()),
        topic_names: opt.topics,
    };

    // return server separately from topic result
    Ok((target_server, describe_topics_cfg))
}
