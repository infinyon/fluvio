//!
//! # Consume CLI
//!
//! CLI command for Consume operation
//!

use std::{io::Error as IoError, path::PathBuf};
use std::io::{self, ErrorKind, Read, Stdout};
use std::collections::{BTreeMap};
use flate2::Compression;
use flate2::bufread::GzEncoder;
use tracing::{debug, trace, instrument};
use structopt::StructOpt;
use structopt::clap::arg_enum;
use fluvio_future::io::StreamExt;
use futures::{select, FutureExt};

mod record_format;
mod table_format;
use table_format::TableModel;

use fluvio::{ConsumerConfig, Fluvio, FluvioError, MultiplePartitionConsumer, Offset};
use fluvio_sc_schema::ApiError;
use fluvio::consumer::{PartitionSelectionStrategy, Record};
use fluvio::consumer::{
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartStreamKind, SmartStreamInvocation,
};

use tui::Terminal;
use tui::backend::CrosstermBackend;
//use crossterm::{
//    event::{DisableMouseCapture, EnableMouseCapture, EventStream},
//    execute,
//    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
//};

use crate::{CliError, Result};
use crate::common::FluvioExtensionMetadata;
use self::record_format::{
    format_text_record, format_binary_record, format_dynamic_record, format_raw_record,
    format_json, format_basic_table_record, format_fancy_table_record,
};
use handlebars::Handlebars;

const DEFAULT_TAIL: u32 = 10;
const USER_TEMPLATE: &str = "user_template";

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

    /// Consume records from all partitions
    #[structopt(short = "A", long = "all-partitions", conflicts_with_all = &["partition"])]
    pub all_partitions: bool,

    /// disable continuous processing of messages
    #[structopt(short = "d", long)]
    pub disable_continuous: bool,

    /// Print records in "[key] value" format, with "[null]" for no key
    #[structopt(short, long)]
    pub key_value: bool,

    /// Provide a template string to print records with a custom format.
    /// See --help for details.
    ///
    /// Template strings may include the variables {{key}}, {{value}}, {{offset}} and {{partition}}
    /// which will have each record's contents substituted in their place.
    /// For example, the following template string:
    ///
    /// Offset {{offset}} has key {{key}} and value {{value}}
    ///
    /// Would produce a printout where records might look like this:
    ///
    /// Offset 0 has key A and value Apple
    #[structopt(short = "F", long, conflicts_with_all = &["output", "key_value"])]
    pub format: Option<String>,

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
    )]
    pub output: Option<ConsumeOutputType>,

    /// Path to a SmartStream filter wasm file
    pub smartstream: Option<String>,

    /// Path to a SmartStream filter wasm file
    #[structopt(long, group("smartmodule"))]
    pub filter: Option<String>,

    /// Path to a SmartStream map wasm file
    #[structopt(long, group("smartmodule"))]
    pub map: Option<String>,

    /// Path to a SmartStream filter_map wasm file
    #[structopt(long, group("smartmodule"))]
    pub filter_map: Option<String>,

    /// Path to a SmartStream array_map wasm file
    #[structopt(long, group("smartmodule"))]
    pub array_map: Option<String>,

    /// Path to a WASM file for aggregation
    #[structopt(long, group("smartmodule"))]
    pub aggregate: Option<String>,

    /// (Optional) Path to a file to use as an initial accumulator value with --aggregate
    #[structopt(long)]
    pub initial: Option<String>,

    /// (Optional) Extra input parameters passed to the smartmodule module.
    /// They should be passed using key=value format
    /// Eg. fluvio consume topic-name --filter filter.wasm -E foo=bar -E key=value -E one=1
    #[structopt(short = "e", long= "extra-params", parse(try_from_str = parse_key_val), number_of_values = 1)]
    pub extra_params: Option<Vec<(String, String)>>,
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s.find('=').ok_or_else(|| {
        CliError::InvalidArg(format!("invalid KEY=value: no `=` found in `{}`", s))
    })?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

impl ConsumeOpt {
    #[instrument(
        skip(self, fluvio),
        name = "Consume",
        fields(topic = %self.topic, partition = self.partition),
    )]
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        if self.all_partitions {
            let consumer = fluvio
                .consumer(PartitionSelectionStrategy::All(self.topic.clone()))
                .await?;
            self.consume_records(consumer).await?;
        } else {
            let consumer = fluvio
                .consumer(PartitionSelectionStrategy::Multiple(vec![(
                    self.topic.clone(),
                    self.partition,
                )]))
                .await?;
            self.consume_records(consumer).await?;
        };

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

    pub async fn consume_records(&self, consumer: MultiplePartitionConsumer) -> Result<()> {
        trace!(config = ?self, "Starting consumer:");
        self.init_ctrlc()?;
        let offset = self.calculate_offset()?;

        let mut builder = ConsumerConfig::builder();
        if let Some(max_bytes) = self.max_bytes {
            builder.max_bytes(max_bytes);
        }

        let extra_params = match &self.extra_params {
            None => BTreeMap::default(),
            Some(params) => params.clone().into_iter().collect(),
        };

        let smartstream = self
            .smartstream
            .as_ref()
            .map(|smartstream_name| SmartStreamInvocation {
                stream: smartstream_name.clone(),
                params: extra_params.clone().into(),
            });

        builder.smartstream(smartstream);

        let smart_module = if let Some(name_or_path) = &self.filter {
            Some(create_smart_module(
                name_or_path,
                SmartStreamKind::Filter,
                extra_params,
            )?)
        } else if let Some(name_or_path) = &self.map {
            Some(create_smart_module(
                name_or_path,
                SmartStreamKind::Map,
                extra_params,
            )?)
        } else if let Some(name_or_path) = &self.array_map {
            Some(create_smart_module(
                name_or_path,
                SmartStreamKind::ArrayMap,
                extra_params,
            )?)
        } else if let Some(name_or_path) = &self.filter_map {
            Some(create_smart_module(
                name_or_path,
                SmartStreamKind::FilterMap,
                extra_params,
            )?)
        } else {
            match (&self.aggregate, &self.initial) {
                (Some(name_or_path), Some(acc_path)) => {
                    let accumulator = std::fs::read(acc_path)?;
                    Some(create_smart_module(
                        name_or_path,
                        SmartStreamKind::Aggregate { accumulator },
                        extra_params,
                    )?)
                }
                (Some(name_or_path), None) => Some(create_smart_module(
                    name_or_path,
                    SmartStreamKind::Aggregate {
                        accumulator: Vec::new(),
                    },
                    extra_params,
                )?),
                (None, Some(_)) => {
                    println!("In order to use --accumulator, you must also specify --aggregate");
                    return Ok(());
                }
                (None, None) => None,
            }
        };

        builder.smart_module(smart_module);

        if self.disable_continuous {
            builder.disable_continuous(true);
        }

        let consume_config = builder.build()?;
        self.consume_records_stream(consumer, offset, consume_config)
            .await?;

        if !self.disable_continuous {
            eprintln!("Consumer stream has closed");
        }

        Ok(())
    }

    /// Consume records as a stream, waiting for new records to arrive
    async fn consume_records_stream(
        &self,
        consumer: MultiplePartitionConsumer,
        offset: Offset,
        config: ConsumerConfig,
    ) -> Result<()> {
        self.print_status();
        let mut stream = consumer.stream_with_config(offset, config).await?;

        let templates = match self.format.as_deref() {
            None => None,
            Some(format) => {
                let mut reg = Handlebars::new();
                reg.register_template_string(USER_TEMPLATE, format)?;
                Some(reg)
            }
        };

        let mut maybe_terminal_stdout = if let Some(ConsumeOutputType::full_table) = &self.output {
            let stdout = io::stdout();
            Some(self.create_terminal(stdout)?)
        } else {
            None
        };

        // This is used by table output, to manage printing the table titles only one time
        let mut header_print = true;

        loop {
            select! {
                stream_next = stream.next().fuse() => match stream_next {
                    Some(result) => {
                        let result: std::result::Result<Record, _> = result;
                        let record = match result {
                            Ok(record) => record,
                            Err(FluvioError::AdminApi(ApiError::Code(code, _))) => {
                                eprintln!("{}", code.to_sentence());
                                continue;
                            }
                            Err(other) => return Err(other.into()),
                        };

                        self.print_record(
                            templates.as_ref(),
                            &record,
                            &mut header_print,
                            &mut maybe_terminal_stdout,
                        );
                    },
                    None => break,
                }
            }
        }

        debug!("fetch loop exited");
        Ok(())
    }

    fn create_terminal(&self, stdout: Stdout) -> Result<Terminal<CrosstermBackend<Stdout>>> {
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;

        Ok(terminal)
    }

    /// Process fetch topic response based on output type
    pub fn print_record(
        &self,
        templates: Option<&Handlebars>,
        record: &Record,
        header_print: &mut bool,
        terminal: &mut Option<Terminal<CrosstermBackend<Stdout>>>,
    ) {
        let formatted_key = record
            .key()
            .map(|key| String::from_utf8_lossy(key).to_string())
            .unwrap_or_else(|| "null".to_string());

        let formatted_value = match (&self.output, templates) {
            (Some(ConsumeOutputType::json), None) => {
                format_json(record.value(), self.suppress_unknown)
            }
            (Some(ConsumeOutputType::text), None) => {
                Some(format_text_record(record.value(), self.suppress_unknown))
            }
            (Some(ConsumeOutputType::binary), None) => Some(format_binary_record(record.value())),
            (Some(ConsumeOutputType::dynamic) | None, None) => {
                Some(format_dynamic_record(record.value()))
            }
            (Some(ConsumeOutputType::raw), None) => Some(format_raw_record(record.value())),
            (Some(ConsumeOutputType::table), None) => {
                let value = format_basic_table_record(record.value(), *header_print);

                // Only print the header once
                if header_print == &true {
                    *header_print = false;
                }

                Some(value)
            }
            (Some(ConsumeOutputType::full_table), None) => {
                let mut table_model = TableModel::default();

                if let Some(term) = terminal {
                    Some(format_fancy_table_record(
                        record.value(),
                        term,
                        &mut table_model,
                    ))
                } else {
                    unreachable!()
                }
            }
            (_, Some(templates)) => {
                let value = String::from_utf8_lossy(record.value()).to_string();
                let object = serde_json::json!({
                    "key": formatted_key,
                    "value": value,
                    "offset": record.offset(),
                    "partition": record.partition(),
                });
                templates.render(USER_TEMPLATE, &object).ok()
            }
        };

        // If the consume type is table, we don't want to accidentally print a newline
        if self.output != Some(ConsumeOutputType::full_table) {
            match formatted_value {
                Some(value) if self.key_value => {
                    println!("[{}] {}", formatted_key, value);
                }
                Some(value) => {
                    println!("{}", value);
                }
                // (Some(_), None) only if JSON cannot be printed, so skip.
                _ => debug!("Skipping record that cannot be formatted"),
            }
        } else {
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

fn create_smart_module(
    name_or_path: &str,
    kind: SmartStreamKind,
    params: BTreeMap<String, String>,
) -> Result<SmartModuleInvocation> {
    let wasm = if PathBuf::from(name_or_path).exists() {
        let raw_buffer = std::fs::read(name_or_path)?;
        debug!(len = raw_buffer.len(), "read wasm bytes");
        let mut encoder = GzEncoder::new(raw_buffer.as_slice(), Compression::default());
        let mut buffer = Vec::with_capacity(raw_buffer.len());
        encoder.read_to_end(&mut buffer)?;
        SmartModuleInvocationWasm::AdHoc(buffer)
    } else {
        SmartModuleInvocationWasm::Predefined(name_or_path.to_owned())
    };

    Ok(SmartModuleInvocation {
        wasm,
        kind,
        params: params.into(),
    })
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
        table,
        full_table,
    }
}

/// Consume output type defaults to text formatting
impl ::std::default::Default for ConsumeOutputType {
    fn default() -> Self {
        ConsumeOutputType::dynamic
    }
}
