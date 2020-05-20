//!
//! # Describe Topic CLI
//!
//! CLI to describe Topics and their corresponding Partitions
//!

use structopt::StructOpt;


use flv_client::profile::ControllerTargetConfig;
use flv_client::profile::ControllerTargetInstance;
use crate::Terminal;
use crate::error::CliError;
use crate::OutputType;
use crate::tls::TlsConfig;
use crate::profile::InlineProfile;

use super::helpers::describe_kf_topics;
use super::helpers::describe_sc_topics;

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct DescribeTopicsConfig {
    pub topic_names: Vec<String>,
    pub output: OutputType,
}

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

    #[structopt(flatten)]
    kf: crate::common::KfConfig,

    /// Output
    #[structopt(
        short = "O",
        long = "output",
        value_name = "type",
        possible_values = &OutputType::variants(),
        case_insensitive = true
    )]
    output: Option<OutputType>,

    #[structopt(flatten)]
    tls: TlsConfig,

    #[structopt(flatten)]
    profile: InlineProfile
}

impl DescribeTopicsOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ControllerTargetConfig, DescribeTopicsConfig), CliError> {
        let target_server = ControllerTargetConfig::possible_target(
            self.sc, 
            #[cfg(kf)]
            self.kf.kf,
        #[cfg(not(foo))]
            None,
            self.tls.try_into_file_config()?,self.profile.profile)?;

        // transfer config parameters
        let describe_topics_cfg = DescribeTopicsConfig {
            output: self.output.unwrap_or(OutputType::default()),
            topic_names: self.topics,
        };

        // return server separately from topic result
        Ok((target_server, describe_topics_cfg))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process describe topic cli request
pub async fn process_describe_topics<O>(
    out: std::sync::Arc<O>,
    opt: DescribeTopicsOpt,
) -> Result<String, CliError>
where
    O: Terminal,
{
    let (target_server, cfg) = opt.validate()?;

    (match target_server.connect().await? {
        ControllerTargetInstance::Kf(client) => {
            describe_kf_topics(client, cfg.topic_names, cfg.output, out).await
        }
        ControllerTargetInstance::Sc(client) => {
            describe_sc_topics(client, cfg.topic_names, cfg.output, out).await
        }
    })
        .map(|_| format!(""))
        .map_err(|err| err.into())
}

// Query Kafka server for T
