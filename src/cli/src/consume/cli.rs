//!
//! # Consume CLI
//!
//! CLI command for Consume operation
//!

use structopt::StructOpt;

use kf_protocol::api::Offset;
use kf_protocol::api::MAX_BYTES;
use flv_client::profile::ServerTargetConfig;

use crate::error::CliError;
use crate::tls::TlsConfig;
use crate::profile::InlineProfile;

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
    #[structopt(short = "d",long)]
    pub disable_continuous: bool,

    /// optional, offset, negate offset is relative to end offset (either committed or uncommitted)
    #[structopt(short,long, value_name = "integer")]
    pub offset: Option<i64>,

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
    tls: TlsConfig,

    #[structopt(flatten)]
    profile: InlineProfile
}

impl ConsumeLogOpt {
    /// validate the configuration and generate target server and config which can be used
    pub fn validate(self) -> Result<(ServerTargetConfig, ConsumeLogConfig), CliError> {

    
        let target_server = ServerTargetConfig::possible_target(
            self.sc, 
        self.spu, 
        self.kf,
        self.tls.try_into_file_config()?,
            self.profile.profile)?;
        let max_bytes = self.max_bytes.unwrap_or(MAX_BYTES);

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
