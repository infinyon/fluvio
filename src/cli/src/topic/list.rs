//!
//! # List Topics CLI
//!
//! CLI tree and processing to list Topics
//!

use structopt::StructOpt;

use log::debug;

use crate::error::CliError;
use crate::OutputType;
use crate::profile::SpuControllerConfig;
use crate::profile::SpuControllerTarget;
use crate::Terminal;

use super::helpers::list_kf_topics;
use super::helpers::list_sc_topics;

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct ListTopicsConfig {
    pub output: OutputType,
}

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

impl ListTopicsOpt {

    /// Validate cli options and generate config
    fn validate(self) -> Result<(SpuControllerConfig, ListTopicsConfig), CliError> {

        let target_server = SpuControllerConfig::new(self.sc, self.kf, self.profile)?;
        
        // transfer config parameters
        let list_topics_cfg = ListTopicsConfig {
            output: self.output.unwrap_or(OutputType::default()),
        };

        // return server separately from topic result
        Ok((target_server, list_topics_cfg))
    }

}



// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process list topics cli request
pub async fn process_list_topics<O>(out: std::sync::Arc<O>,opt: ListTopicsOpt) -> Result<String, CliError> 
    where O: Terminal
{

    let (target_server, cfg) = opt.validate()?;

    debug!("list topics {:#?} server: {:#?}",cfg,target_server);

    (match target_server.connect().await? {
        SpuControllerTarget::Kf(client) => list_kf_topics(out,client, cfg.output).await,
        SpuControllerTarget::Sc(client) => list_sc_topics(out,client, cfg.output).await
    })
        .map(|_| format!(""))
        .map_err(|err| err.into())
}

