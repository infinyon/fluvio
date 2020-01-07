//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!

use structopt::StructOpt;

use flv_client::SpuController;

use crate::error::CliError;
use flv_client::profile::SpuControllerConfig;
use flv_client::profile::SpuControllerTarget;

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct DeleteTopicConfig {
    pub name: String,
}

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

impl DeleteTopicOpt {
    /// Validate cli options. Generate target-server and delete-topic configuration.
    fn validate(self) -> Result<(SpuControllerConfig, DeleteTopicConfig), CliError> {
        // profile specific configurations (target server)
        let target_server = SpuControllerConfig::new(self.sc, self.kf, self.profile)?;
        let delete_topic_cfg = DeleteTopicConfig { name: self.topic };

        // return server separately from config
        Ok((target_server, delete_topic_cfg))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete topic cli request
pub async fn process_delete_topic(opt: DeleteTopicOpt) -> Result<String, CliError> {
    let (target_server, cfg) = opt.validate()?;

    (match target_server.connect().await? {
        SpuControllerTarget::Kf(mut client) => client.delete_topic(&cfg.name).await,
        SpuControllerTarget::Sc(mut client) => client.delete_topic(&cfg.name).await,
    })
    .map(|topic_name| format!("topic \"{}\" deleted", topic_name))
    .map_err(|err| err.into())
}
