//!
//! # Consume CLI
//!
//! CLI command for Consume operation
//!

use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use kf_protocol::api::Offset;
use kf_protocol::api::MAX_BYTES;

use crate::error::CliError;
use crate::common::ConsumeOutputType;
use crate::profile::{ProfileConfig, TargetServer};

use super::ReponseLogParams;

use super::sc_consume_log_from_topic;
use super::sc_consume_log_from_topic_partition;
use super::spu_consume_log_from_topic_partition;
use super::kf_consume_log_from_topic;
use super::kf_consume_log_from_topic_partition;

#[derive(Debug, StructOpt)]
pub struct ConsumeLogOpt {
    /// Topic name
    #[structopt(short = "t", long = "topic", value_name = "string")]
    pub topic: String,

    /// Partition id
    #[structopt(short = "p", long = "partition", value_name = "integer")]
    pub partition: Option<i32>,

    /// Start reading from this offset
    #[structopt(short = "g", long = "from-beginning")]
    pub from_beginning: bool,

    /// Read messages in a infinite loop
    #[structopt(short = "C", long = "continuous")]
    pub continuous: bool,

    /// Maximum number of bytes to be retrieved
    #[structopt(short = "b", long = "maxbytes", value_name = "integer")]
    pub max_bytes: Option<i32>,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    pub sc: Option<String>,

    /// Address of Streaming Processing Unit
    #[structopt(
        short = "u",
        long = "spu",
        value_name = "host:port",
        conflicts_with = "sc"
    )]
    pub spu: Option<String>,

    /// Address of Kafka Controller
    #[structopt(
        short = "k",
        long = "kf",
        value_name = "host:port",
        conflicts_with = "sc",
        conflicts_with = "spu"
    )]
    pub kf: Option<String>,

    /// Profile name
    #[structopt(short = "P", long = "profile")]
    pub profile: Option<String>,

    /// Suppress items items that have an unknown output type
    #[structopt(short = "s", long = "suppress-unknown")]
    pub suppress_unknown: bool,

    /// Output
    #[structopt(
        short = "O",
        long = "output",
        value_name = "type",
        raw(
            possible_values = "&ConsumeOutputType::variants()",
            case_insensitive = "true"
        )
    )]
    output: Option<ConsumeOutputType>,
}

// -----------------------------------
//  Parsed Config
// -----------------------------------

/// Consume log configuration parameters
#[derive(Debug)]
pub struct ConsumeLogConfig {
    pub topic: String,
    pub partition: Option<i32>,
    pub from_beginning: bool,
    pub continous: bool,
    pub offset: Offset,
    pub max_bytes: i32,

    pub output: ConsumeOutputType,
    pub suppress_unknown: bool,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process Consume log cli request
pub fn process_consume_log(opt: ConsumeLogOpt) -> Result<(), CliError> {
    let (target_server, consume_log_cfg) = parse_opt(opt)?;

    // setup response formatting
    let response_params = ReponseLogParams {
        output: consume_log_cfg.output.clone(),
        suppress: consume_log_cfg.suppress_unknown,
    };

    if let Some(partition) = consume_log_cfg.partition {
        // consume one topic/partition
        match target_server {
            TargetServer::Sc(server_addr) => sc_consume_log_from_topic_partition(
                server_addr,
                consume_log_cfg,
                partition,
                response_params,
            ),
            TargetServer::Spu(server_addr) => spu_consume_log_from_topic_partition(
                server_addr,
                consume_log_cfg,
                partition,
                response_params,
            ),
            TargetServer::Kf(server_addr) => kf_consume_log_from_topic_partition(
                server_addr,
                consume_log_cfg,
                partition,
                response_params,
            ),
        }
    } else {
        // consume one topic/all partitions
        match target_server {
            TargetServer::Sc(server_addr) => {
                sc_consume_log_from_topic(server_addr, consume_log_cfg, response_params)
            }
            TargetServer::Kf(server_addr) => {
                kf_consume_log_from_topic(server_addr, consume_log_cfg, response_params)
            }
            _ => Err(CliError::IoError(IoError::new(
                ErrorKind::Other,
                format!("Partition index is required for reading logs form SPU."),
            ))),
        }
    }
}

/// Validate cli options. Generate target-server and consume log configuration.
fn parse_opt(opt: ConsumeLogOpt) -> Result<(TargetServer, ConsumeLogConfig), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new_with_spu(&opt.sc, &opt.spu, &opt.kf, &opt.profile)?;
    let target_server = profile_config.target_server()?;
    let max_bytes = opt.max_bytes.unwrap_or(MAX_BYTES);

    // consume log specific configurations
    let consume_log_cfg = ConsumeLogConfig {
        topic: opt.topic,
        partition: opt.partition,
        from_beginning: opt.from_beginning,
        continous: opt.continuous,
        offset: -1,
        max_bytes: max_bytes,

        output: opt.output.unwrap_or(ConsumeOutputType::default()),
        suppress_unknown: opt.suppress_unknown,
    };

    // return server separately from config
    Ok((target_server, consume_log_cfg))
}
