//!
//! # Describe Topic CLI
//!
//! CLI to describe Topics and their corresponding Partitions
//!


use structopt::StructOpt;

use crate::error::CliError;
use crate::OutputType;
use crate::profile::SpuControllerConfig;
use crate::profile::SpuControllerTarget;
use crate::Terminal;


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

impl DescribeTopicsOpt {

    /// Validate cli options and generate config
    fn validate(self) -> Result<(SpuControllerConfig, DescribeTopicsConfig), CliError> {

        let target_server = SpuControllerConfig::new(self.sc, self.kf, self.profile)?;

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
pub async fn process_describe_topics<O>(out: std::sync::Arc<O>,opt: DescribeTopicsOpt) -> Result<String, CliError> 
    where O: Terminal
{

    let (target_server, cfg) = opt.validate()?;

    (match target_server.connect().await? {
        SpuControllerTarget::Kf(client) => describe_kf_topics(client, cfg.topic_names,cfg.output,out).await,
        SpuControllerTarget::Sc(client) => describe_sc_topics(client, cfg.topic_names,cfg.output,out).await
    })
        .map(|_| format!(""))
        .map_err(|err| err.into())
}




// Query Kafka server for T