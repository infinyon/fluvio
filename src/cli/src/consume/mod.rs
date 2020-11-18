//!
//! # Consume CLI
//!
//! CLI command for Consume operation
//!

use std::sync::Arc;
use structopt::StructOpt;
use structopt::clap::arg_enum;

mod fetch_log_loop;
mod logs_output;

use fluvio::Fluvio;
use crate::error::CliError;
use crate::consume::fetch_log_loop::fetch_log_loop;
use crate::Terminal;

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
    pub output: ConsumeOutputType,
}

impl ConsumeLogOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<(), CliError> {
        let consumer = fluvio
            .partition_consumer(&self.topic, self.partition)
            .await?;
        fetch_log_loop(out, consumer, self).await?;
        Ok(())
    }
}

// Uses clap::arg_enum to choose possible variables
arg_enum! {
    #[derive(Debug, Clone, PartialEq)]
    #[allow(non_camel_case_types)]
    pub enum ConsumeOutputType {
        dynamic,
        text,
        binary,
        json,
        raw,
    }
}

/// Consume output type defaults to text formatting
impl ::std::default::Default for ConsumeOutputType {
    fn default() -> Self {
        ConsumeOutputType::dynamic
    }
}
