//!
//! # Delete Topics
//!
//! CLI tree to generate Delete Topics
//!

use structopt::StructOpt;

use flv_client::SpuController;
use flv_client::profile::ScConfig;

use crate::error::CliError;
use crate::tls::TlsConfig;


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

    #[structopt(flatten)]
    tls: TlsConfig,
}

impl DeleteTopicOpt {
    /// Validate cli options. Generate target-server and delete-topic configuration.
    fn validate(self) -> Result<(ScConfig, DeleteTopicConfig), CliError> {
        // profile specific configurations (target server)
        let target_server = ScConfig::new(self.sc,self.tls.try_into_file_config()?)?;
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

    let mut client = target_server.connect().await?;
    client.delete_topic(&cfg.name).await
        .map(|topic_name| format!("topic \"{}\" deleted", topic_name))
        .map_err(|err| err.into())
}
