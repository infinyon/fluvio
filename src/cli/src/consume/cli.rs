//!
//! # Consume CLI
//!
//! CLI command for Consume operation
//!

use structopt::StructOpt;

use kf_protocol::api::Offset;
use kf_protocol::api::MAX_BYTES;

use crate::error::CliError;
use flv_client::profile::ReplicaLeaderConfig;

use super::ConsumeOutputType;

#[derive(Debug, StructOpt)]
pub struct ConsumeLogOpt {
    /// Topic name
    #[structopt(short = "t", long = "topic", value_name = "string")]
    pub topic: String,

    /// Partition id
    #[structopt(short = "p", long = "partition", value_name = "integer")]
    pub partition: i32,

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

impl ConsumeLogOpt {
    /// validate the configuration and generate target server and config which can be used
    pub fn validate(self) -> Result<(ReplicaLeaderConfig, ConsumeLogConfig), CliError> {
        // profile specific configurations (target server)
        let target_server = ReplicaLeaderConfig::new(self.sc, self.spu, self.kf, self.profile)?;
        let max_bytes = self.max_bytes.unwrap_or(MAX_BYTES);

        // consume log specific configurations
        let consume_log_cfg = ConsumeLogConfig {
            topic: self.topic,
            partition: self.partition,
            from_beginning: self.from_beginning,
            continuous: self.continuous,
            offset: -1,
            max_bytes: max_bytes,

            output: self.output.unwrap_or(ConsumeOutputType::default()),
            suppress_unknown: self.suppress_unknown,
        };

        // return server separately from config
        Ok((target_server, consume_log_cfg))
    }
}

/// Consume log configuration parameters
#[derive(Debug)]
pub struct ConsumeLogConfig {
    pub topic: String,
    pub partition: i32,
    pub from_beginning: bool,
    pub continuous: bool,
    pub offset: Offset,
    pub max_bytes: i32,

    pub output: ConsumeOutputType,
    pub suppress_unknown: bool,
}
