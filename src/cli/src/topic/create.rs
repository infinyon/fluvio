//!
//! # Create Topics
//!
//! CLI tree to generate Create Topics
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;

use structopt::StructOpt;

use flv_client::client::*;
use flv_client::query_params::ReplicaConfig;
use flv_client::query_params::Partitions;
use flv_client::profile::ControllerTargetConfig;
use flv_client::profile::ControllerTargetInstance;

use crate::error::CliError;
use crate::tls::TlsConfig;
use crate::profile::InlineProfile;

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct CreateTopicConfig {
    pub topic: String,
    pub replica: ReplicaConfig,
    pub validate_only: bool,
}

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct CreateTopicOpt {
    /// Topic name
    #[structopt(value_name = "topic name")]
    topic: String,

    /// Number of partitions
    #[structopt(
        short = "p",
        long = "partitions",
        value_name = "partitions",
        default_value = "1",
        required_unless = "replica_assignment"
    )]
    partitions: i32,

    /// Replication factor per partition
    #[structopt(
        short = "r",
        long = "replication",
        value_name = "integer",
        default_value = "1",
        required_unless = "replica_assignment"
    )]
    replication: i16,

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

    #[structopt(flatten)]
    kf: crate::common::KfConfig,

    #[structopt(flatten)]
    tls: TlsConfig,

    #[structopt(flatten)]
    profile: InlineProfile,
}

impl CreateTopicOpt {
    /// Ensure all parameters are valid for computed replication
    fn parse_computed_replica(&self) -> ReplicaConfig {
        ReplicaConfig::Computed(
            self.partitions,
            self.replication,
            self.ignore_rack_assigment,
        )
    }

    /// Ensure all parameters are valid for computed replication
    fn parse_assigned_replica(&self) -> Result<ReplicaConfig, CliError> {
        if let Some(replica_assign_file) = &self.replica_assignment {
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

    /// Validate cli options. Generate target-server and create-topic configuration.
    fn validate(self) -> Result<(ControllerTargetConfig, CreateTopicConfig), CliError> {
        // topic specific configurations
        let replica_config = if self.replica_assignment.is_none() {
            self.parse_computed_replica()
        } else {
            self.parse_assigned_replica()?
        };

        let create_topic_cfg = CreateTopicConfig {
            topic: self.topic,
            replica: replica_config,
            validate_only: self.validate_only,
        };

        let target_server = ControllerTargetConfig::possible_target(
            self.sc,
            #[cfg(feature = "kf")]
            self.kf.kf,
            #[cfg(not(feature = "kf"))]
            None,
            self.tls.try_into_file_config()?,
            self.profile.profile,
        )?;

        // return server separately from config
        Ok((target_server, create_topic_cfg))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process create topic cli request
pub async fn process_create_topic(opt: CreateTopicOpt) -> Result<String, CliError> {
    let (target_server, cfg) = opt.validate()?;

    (match target_server.connect().await? {
        ControllerTargetInstance::Kf(mut client) => {
            client
                .create_topic(cfg.topic, cfg.replica, cfg.validate_only)
                .await
        }
        ControllerTargetInstance::Sc(mut client) => {
            client
                .create_topic(cfg.topic, cfg.replica, cfg.validate_only)
                .await
        }
    })
    .map(|topic_name| format!("topic \"{}\" created", topic_name))
    .map_err(|err| err.into())
}
