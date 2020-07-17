//!
//! # Consume CLI
//!
//! CLI command for Consume operation
//!

use structopt::StructOpt;

use flv_client::kf::api::Offset;
use flv_client::ClusterConfig;
use flv_client::params::MAX_FETCH_BYTES;

use crate::error::CliError;
use crate::target::ClusterTarget;

use super::ConsumeOutputType;

#[derive(Debug, StructOpt)]
pub struct ConsumeLogOpt {
    /// Topic name
    #[structopt(value_name = "string")]
    pub topic: String,

    /// Partition id
    #[structopt(short = "p", long, default_value = "0", value_name = "integer")]
    pub partition: i32,

    /// Start reading from beginning
    #[structopt(short = "B", long = "from-beginning")]
    pub from_beginning: bool,

    /// disable continuous processing of messages
    #[structopt(short = "d", long)]
    pub disable_continuous: bool,

    /// Offsets can be positive or negative. (Syntax for negative offset: --offset="-1")
    #[structopt(short, long, value_name = "integer")]
    pub offset: Option<i64>,

    /// Maximum number of bytes to be retrieved
    #[structopt(short = "b", long = "maxbytes", value_name = "integer")]
    pub max_bytes: Option<i32>,

  
    /// Suppress items items that have an unknown output type
    #[structopt(short = "s", long = "suppress-unknown")]
    pub suppress_unknown: bool,

    /// Output
    #[structopt(
        short = "O",
        long = "output",
        value_name = "type",
        possible_values = &ConsumeOutputType::variants(),
        case_insensitive = true,
        default_value
    )]
    output: ConsumeOutputType,

    #[structopt(flatten)]
    target: ClusterTarget
}

impl ConsumeLogOpt {
    /// validate the configuration and generate target server and config which can be used
    pub fn validate(self) -> Result<(ClusterConfig, ConsumeLogConfig), CliError> {
        let target_server = self.target.load()?;
        let max_bytes = self.max_bytes.unwrap_or(MAX_FETCH_BYTES as i32);

        // consume log specific configurations
        let consume_log_cfg = ConsumeLogConfig {
            topic: self.topic,
            partition: self.partition,
            from_beginning: self.from_beginning,
            disable_continuous: self.disable_continuous,
            offset: self.offset,
            max_bytes: max_bytes,
            output: self.output,
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
    pub disable_continuous: bool,
    pub offset: Option<Offset>,
    pub max_bytes: i32,
    pub output: ConsumeOutputType,
    pub suppress_unknown: bool,
}
