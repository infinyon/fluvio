//!
//! # Create Topics
//!
//! CLI tree to generate Create Topics
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;

use structopt::StructOpt;

use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::Partitions;
use super::helpers::process_sc_create_topic;
use super::helpers::process_kf_create_topic;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct CreateTopicOpt {
    /// Topic name
    #[structopt(short = "t", long = "topic", value_name = "string")]
    topic: String,

    /// Number of partitions
    #[structopt(
        short = "p",
        long = "partitions",
        value_name = "integer",
        required_unless = "replica_assignment"
    )]
    partitions: Option<i32>,

    /// Replication factor per partition
    #[structopt(
        short = "r",
        long = "replication",
        value_name = "integer",
        required_unless = "replica_assignment"
    )]
    replication: Option<i16>,

    /// Ignore racks while computing replica assignment
    #[structopt(
        short = "i",
        long = "ignore-rack-assignment",
        conflicts_with = "replica_assignment"
    )]
    ignore_rack_assigment: bool,

    /// Replica assignment file
    #[structopt(
        short = "f",
        long = "replica-assignment",
        value_name = "file.json",
        parse(from_os_str),
        conflicts_with = "partitions",
        conflicts_with = "replication"
    )]
    replica_assignment: Option<PathBuf>,

    /// Validates configuration, does not provision
    #[structopt(short = "v", long = "validate-only")]
    validate_only: bool,

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
pub struct CreateTopicConfig {
    pub name: String,
    pub replica: ReplicaConfig,
    pub validate_only: bool,
}

#[derive(Debug)]
pub enum ReplicaConfig {
    // replica assignment
    Assigned(Partitions),

    // partitions, replication, ignore_rack_assignment
    Computed(i32, i16, bool),
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process create topic cli request
pub fn process_create_topic(opt: CreateTopicOpt) -> Result<(), CliError> {
    let (target_server, create_topic_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Kf(server_addr) => process_kf_create_topic(server_addr, create_topic_cfg),
        TargetServer::Sc(server_addr) => process_sc_create_topic(server_addr, create_topic_cfg),
        _ => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("invalid target server {:?}", target_server),
        ))),
    }
}

/// Validate cli options. Generate target-server and create-topic configuration.
fn parse_opt(opt: CreateTopicOpt) -> Result<(TargetServer, CreateTopicConfig), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new(&opt.sc, &opt.kf, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // topic specific configurations
    let replica_config = if opt.partitions.is_some() {
        parse_computed_replica(&opt)?
    } else {
        parse_assigned_replica(&opt)?
    };
    let create_topic_cfg = CreateTopicConfig {
        name: opt.topic,
        replica: replica_config,
        validate_only: opt.validate_only,
    };

    // return server separately from config
    Ok((target_server, create_topic_cfg))
}

/// Ensure all parameters are valid for computed replication
fn parse_computed_replica(opt: &CreateTopicOpt) -> Result<ReplicaConfig, CliError> {
    Ok(ReplicaConfig::Computed(
        opt.partitions.unwrap_or(-1),
        opt.replication.unwrap_or(-1),
        opt.ignore_rack_assigment,
    ))
}

/// Ensure all parameters are valid for computed replication
fn parse_assigned_replica(opt: &CreateTopicOpt) -> Result<ReplicaConfig, CliError> {
    if let Some(replica_assign_file) = &opt.replica_assignment {
        match Partitions::file_decode(replica_assign_file) {
            Ok(partitions) => Ok(ReplicaConfig::Assigned(partitions)),
            Err(err) => Err(CliError::IoError(IoError::new(
                ErrorKind::InvalidInput,
                format!(
                    "cannot parse replica assignment file {:?}: {}",
                    replica_assign_file, err
                ),
            ))),
        }
    } else {
        Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidInput,
            "cannot find replica assignment file",
        )))
    }
}
