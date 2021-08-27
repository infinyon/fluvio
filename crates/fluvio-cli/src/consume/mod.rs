//!
//! # Consume CLI
//!
//! CLI command for Consume operation
//!

use std::{io::Error as IoError, path::PathBuf};
use std::io::ErrorKind;
use tracing::{debug, trace, instrument};
use structopt::StructOpt;
use structopt::clap::arg_enum;
use fluvio_future::io::StreamExt;

mod record_format;

use fluvio::{Fluvio, PartitionConsumer, Offset, ConsumerConfig, FluvioError};
use fluvio_sc_schema::ApiError;
use fluvio::consumer::Record;

use crate::Result;
use crate::common::FluvioExtensionMetadata;
use self::record_format::{
    format_text_record, format_binary_record, format_dynamic_record, format_raw_record, format_json,
};

const DEFAULT_TAIL: u32 = 10;

/// Read messages from a topic/partition
///
/// By default, consume operates in "streaming" mode, where the command will remain
/// active and wait for new messages, printing them as they arrive. You can use the
/// '-d' flag to exit after consuming all available messages.
#[derive(Debug, StructOpt)]
pub struct ConsumeOpt {
    /// Topic name
    #[structopt(value_name = "topic")]
    pub topic: String,

    /// Partition id
    #[structopt(short = "p", long, default_value = "0", value_name = "integer")]
    pub partition: i32,

    /// disable continuous processing of messages
    #[structopt(short = "d", long)]
    pub disable_continuous: bool,

    /// Print records in "[key] value" format, with "[null]" for no key
    #[structopt(short, long)]
    pub key_value: bool,

    /// Consume records starting X from the beginning of the log (default: 0)
    #[structopt(short = "B", value_name = "integer", conflicts_with_all = &["offset", "tail"])]
    pub from_beginning: Option<Option<u32>>,

    /// The offset of the first record to begin consuming from
    #[structopt(short, long, value_name = "integer", conflicts_with_all = &["from_beginning", "tail"])]
    pub offset: Option<u32>,

    /// Consume records starting X from the end of the log (default: 10)
    #[structopt(short = "T", long, value_name = "integer", conflicts_with_all = &["from_beginning", "offset"])]
    pub tail: Option<Option<u32>>,

    /// Maximum number of bytes to be retrieved
    #[structopt(short = "b", long = "maxbytes", value_name = "integer")]
    pub max_bytes: Option<i32>,

    /// Suppress items items that have an unknown output type
    #[structopt(long = "suppress-unknown")]
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

    /// Path to a SmartStream filter wasm file
    #[structopt(long, group("smartstream"))]
    pub filter: Option<PathBuf>,

    /// Path to a SmartStream map wasm file
    #[structopt(long, group("smartstream"))]
    pub map: Option<PathBuf>,

    /// Path to a WASM file for aggregation
    #[structopt(long, group("smartstream"))]
    pub aggregate: Option<PathBuf>,

    /// (Optional) Path to a file to use as an initial accumulator value with --aggregate
    #[structopt(long)]
    pub initial: Option<PathBuf>,
}

impl ConsumeOpt {
    #[instrument(
        skip(self, fluvio),
        name = "Consume",
        fields(topic = %self.topic, partition = self.partition),
    )]
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let consumer = fluvio
            .partition_consumer(&self.topic, self.partition)
            .await?;
        self.consume_records(consumer).await?;
        Ok(())
    }

    pub fn metadata() -> FluvioExtensionMetadata {
        FluvioExtensionMetadata {
            title: "consume".into(),
            package: Some("fluvio/fluvio".parse().unwrap()),
            description: "Consume new data in a stream".into(),
            version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
        }
    }

    pub async fn consume_records(&self, consumer: PartitionConsumer) -> Result<()> {
        trace!(config = ?self, "Starting consumer:");
        self.init_ctrlc()?;
        let offset = self.calculate_offset()?;

        let mut builder = ConsumerConfig::builder();
        if let Some(max_bytes) = self.max_bytes {
            builder.max_bytes(max_bytes);
        }

        if let Some(filter_path) = &self.filter {
            let buffer = std::fs::read(filter_path)?;
            debug!(len = buffer.len(), "read filter bytes");
            builder.wasm_filter(buffer);
        }

        if let Some(map_path) = &self.map {
            let buffer = std::fs::read(map_path)?;
            debug!(len = buffer.len(), "read filter bytes");
            builder.wasm_map(buffer);
        }

        match (&self.aggregate, &self.initial) {
            (Some(wasm_path), Some(acc_path)) => {
                let wasm = std::fs::read(wasm_path)?;
                let acc = std::fs::read(acc_path)?;
                builder.wasm_aggregate(wasm, acc);
            }
            (Some(wasm_path), None) => {
                let wasm = std::fs::read(wasm_path)?;
                builder.wasm_aggregate(wasm, Vec::new());
            }
            (None, Some(_)) => {
                println!("In order to use --accumulator, you must also specify --aggregate");
                return Ok(());
            }
            (None, None) => (),
        }

        if self.disable_continuous {
            builder.disable_continuous(true);
        }

        let consume_config = builder.build()?;
        self.consume_records_stream(&consumer, offset, consume_config)
            .await?;

        if !self.disable_continuous {
            eprintln!("Consumer stream has closed");
        }

        Ok(())
    }

    /// Consume records as a stream, waiting for new records to arrive
    async fn consume_records_stream(
        &self,
        consumer: &PartitionConsumer,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<()> {
        self.print_status();
        let mut stream = consumer.stream_with_config(offset, config).await?;

        while let Some(result) = stream.next().await {
            let result: std::result::Result<Record, _> = result;
            let record = match result {
                Ok(record) => record,
                Err(FluvioError::AdminApi(ApiError::Code(code, _))) => {
                    eprintln!("{}", code.to_sentence());
                    continue;
                }
                Err(other) => return Err(other.into()),
            };

            self.print_record(record.key(), record.value());
        }

        debug!("fetch loop exited");
        Ok(())
    }

    /// Process fetch topic response based on output type
    pub fn print_record(&self, key: Option<&[u8]>, value: &[u8]) {
        let formatted_key = key.map(|key| {
            String::from_utf8(key.to_owned())
                .unwrap_or_else(|_| "<cannot print non-UTF8 key>".to_string())
        });

        let formatted_value = match self.output {
            ConsumeOutputType::json => format_json(value, self.suppress_unknown),
            ConsumeOutputType::text => Some(format_text_record(value, self.suppress_unknown)),
            ConsumeOutputType::binary => Some(format_binary_record(value)),
            ConsumeOutputType::dynamic => Some(format_dynamic_record(value)),
            ConsumeOutputType::raw => Some(format_raw_record(value)),
        };

        match (formatted_key, formatted_value) {
            (Some(key), Some(value)) if self.key_value => {
                println!("[{}] {}", key, value);
            }
            (None, Some(value)) if self.key_value => {
                println!("[null] {}", value);
            }
            (_, Some(value)) => {
                println!("{}", value);
            }
            // (Some(_), None) only if JSON cannot be printed, so skip.
            _ => debug!("Skipping record that cannot be formatted"),
        }
    }

    fn print_status(&self) {
        use colored::*;
        if !atty::is(atty::Stream::Stdout) {
            return;
        }

        // If --from-beginning=X
        if let Some(Some(offset)) = self.from_beginning {
            eprintln!(
                "{}",
                format!(
                    "Consuming records starting {} from the beginning of topic '{}'",
                    offset, &self.topic
                )
                .bold()
            );
        // If --from-beginning
        } else if let Some(None) = self.from_beginning {
            eprintln!(
                "{}",
                format!(
                    "Consuming records from the beginning of topic '{}'",
                    &self.topic
                )
                .bold()
            );
        // If --offset=X
        } else if let Some(offset) = self.offset {
            eprintln!(
                "{}",
                format!(
                    "Consuming records from offset {} in topic '{}'",
                    offset, &self.topic
                )
                .bold()
            );
        // If --tail or --tail=X
        } else if let Some(maybe_tail) = self.tail {
            let tail = maybe_tail.unwrap_or(DEFAULT_TAIL);
            eprintln!(
                "{}",
                format!(
                    "Consuming records starting {} from the end of topic '{}'",
                    tail, &self.topic
                )
                .bold()
            );
        // If no offset config is given, read from the end
        } else {
            eprintln!(
                "{}",
                format!(
                    "Consuming records from the end of topic '{}'. This will wait for new records",
                    &self.topic
                )
                .bold()
            );
        }
    }

    /// Initialize Ctrl-C event handler
    fn init_ctrlc(&self) -> Result<()> {
        let result = ctrlc::set_handler(move || {
            debug!("detected control c, setting end");
            std::process::exit(0);
        });

        if let Err(err) = result {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!("CTRL-C handler can't be initialized {}", err),
            )
            .into());
        }
        Ok(())
    }

    /// Calculate the Offset to use with the consumer based on the provided offset number
    fn calculate_offset(&self) -> Result<Offset> {
        let offset = if let Some(maybe_offset) = self.from_beginning {
            let offset = maybe_offset.unwrap_or(0);
            Offset::from_beginning(offset)
        } else if let Some(offset) = self.offset {
            Offset::absolute(offset as i64).unwrap()
        } else if let Some(maybe_tail) = self.tail {
            let tail = maybe_tail.unwrap_or(DEFAULT_TAIL);
            Offset::from_end(tail)
        } else {
            Offset::end()
        };

        Ok(offset)
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
