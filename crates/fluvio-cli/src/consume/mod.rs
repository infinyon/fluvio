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
use fluvio_controlplane_metadata::tableformat::{TableFormatSpec};
use tracing::{debug, trace, instrument};
use structopt::StructOpt;
use structopt::clap::arg_enum;
use fluvio_future::io::StreamExt;
use futures::{select, FutureExt};

mod record_format;
mod table_format;
use table_format::{TableEventResponse, TableModel};

use fluvio::{ConsumerConfig, Fluvio, MultiplePartitionConsumer, Offset};
use fluvio::consumer::{PartitionSelectionStrategy, Record};
use fluvio::consumer::{
    SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind, DerivedStreamInvocation,
};

use tui::Terminal;
use tui::backend::CrosstermBackend;
use crossterm::tty::IsTty;
use crossterm::{
    event::EventStream,
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

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

    /// Consume records using the formatting rules defined by TableFormat name
    #[structopt(long, conflicts_with_all = &["key_value", "format"])]
    pub tableformat: Option<String>,

    /// Consume records starting X from the beginning of the log (default: 0)
    #[structopt(short = "B", value_name = "integer", conflicts_with_all = &["offset", "tail"])]
    pub from_beginning: Option<Option<u32>>,

    /// The offset of the first record to begin consuming from
    #[structopt(short, long, value_name = "integer", conflicts_with_all = &["from_beginning", "tail"])]
    pub offset: Option<u32>,

    /// Consume records starting X from the end of the log (default: 10)
    #[structopt(short = "T", long, value_name = "integer", conflicts_with_all = &["from_beginning", "offset"])]
    pub tail: Option<Option<u32>>,

    /// Consume records until end offset
    #[structopt(long, value_name= "integer", conflicts_with_all = &["tail"])]
    pub end_offset: Option<i64>,

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

    /// Name of DerivedStream
    #[structopt(long)]
    pub derivedstream: Option<String>,

    /// Path to a SmartModule filter wasm file
    #[structopt(long, group("smartmodule"))]
    pub filter: Option<String>,

    /// Path to a SmartModule map wasm file
    #[structopt(long, group("smartmodule"))]
    pub map: Option<String>,

    /// Path to a SmartModule filter_map wasm file
    #[structopt(long, group("smartmodule"))]
    pub filter_map: Option<String>,

    /// Path to a SmartModule array_map wasm file
    #[structopt(long, group("smartmodule"))]
    pub array_map: Option<String>,

    /// Path to a SmartModule join wasm filee
    #[structopt(long, group("smartmodule"))]
    pub join: Option<String>,

    /// Path to a WASM file for aggregation
    #[structopt(long, group("smartmodule"))]
    pub aggregate: Option<String>,

    #[structopt(long)]
    pub join_topic: Option<String>,

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
        let maybe_tableformat = if let Some(ref tableformat_name) = self.tableformat {
            let admin = fluvio.admin().await;
            let tableformats = admin.list::<TableFormatSpec, _>(vec![]).await?;

            let mut found = None;

            if !tableformats.is_empty() {
                for t in tableformats {
                    if t.name.as_str() == tableformat_name {
                        //println!("debug: Found tableformat: {:?}", t.spec);
                        found = Some(t.spec);

                        //let tableformat_test = TableFormatSpec {
                        //    name: "hardcoded_test".to_string(),
                        //    columns: Some(vec![
                        //        TableColumn {
                        //            key_path: "key2".to_string(),
                        //            header_label: Some("Key #2".to_string()),
                        //            ..Default::default()
                        //        },
                        //        TableColumn {
                        //            key_path: "key1".to_string(),
                        //            //primary_key: true,
                        //            header_label: Some("Key #1".to_string()),
                        //            ..Default::default()
                        //        },
                        //    ]),
                        //    ..Default::default()
                        //};
                        //println!("debug: Using test tableformat: {:?}", tableformat_test);
                        //found = Some(tableformat_test);
                        break;
                    }
                }

                if found.is_none() {
                    return Err(CliError::TableFormatNotFound(tableformat_name.to_string()));
                }

                found
            } else {
                return Err(CliError::TableFormatNotFound(tableformat_name.to_string()));
            }
        } else {
            None
        };

        if self.all_partitions {
            let consumer = fluvio
                .consumer(PartitionSelectionStrategy::All(self.topic.clone()))
                .await?;
            self.consume_records(consumer, maybe_tableformat).await?;
        } else {
            let consumer = fluvio
                .consumer(PartitionSelectionStrategy::Multiple(vec![(
                    self.topic.clone(),
                    self.partition,
                )]))
                .await?;
            self.consume_records(consumer, maybe_tableformat).await?;
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

    pub async fn consume_records(
        &self,
        consumer: MultiplePartitionConsumer,
        tableformat: Option<TableFormatSpec>,
    ) -> Result<()> {
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

        let derivedstream =
            self.derivedstream
                .as_ref()
                .map(|derivedstream_name| DerivedStreamInvocation {
                    stream: derivedstream_name.clone(),
                    params: extra_params.clone().into(),
                });

        builder.derivedstream(derivedstream);

        let smartmodule = if let Some(name_or_path) = &self.filter {
            Some(create_smartmodule(
                name_or_path,
                SmartModuleKind::Filter,
                extra_params,
            )?)
        } else if let Some(name_or_path) = &self.map {
            Some(create_smartmodule(
                name_or_path,
                SmartModuleKind::Map,
                extra_params,
            )?)
        } else if let Some(name_or_path) = &self.array_map {
            Some(create_smartmodule(
                name_or_path,
                SmartModuleKind::ArrayMap,
                extra_params,
            )?)
        } else if let Some(name_or_path) = &self.filter_map {
            Some(create_smartmodule(
                name_or_path,
                SmartModuleKind::FilterMap,
                extra_params,
            )?)
        } else if let Some(name_or_path) = &self.join {
            Some(create_smartmodule(
                name_or_path,
                SmartModuleKind::Join(
                    self.join_topic
                        .as_ref()
                        .expect("Join topic field is required when using join")
                        .to_owned(),
                ),
                extra_params,
            )?)
        } else {
            match (&self.aggregate, &self.initial) {
                (Some(name_or_path), Some(acc_path)) => {
                    let accumulator = std::fs::read(acc_path)?;
                    Some(create_smartmodule(
                        name_or_path,
                        SmartModuleKind::Aggregate { accumulator },
                        extra_params,
                    )?)
                }
                (Some(name_or_path), None) => Some(create_smartmodule(
                    name_or_path,
                    SmartModuleKind::Aggregate {
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

        builder.smartmodule(smartmodule);

        if self.disable_continuous {
            builder.disable_continuous(true);
        }

        if let Some(end_offset) = self.end_offset {
            if let Some(offset) = self.offset {
                let o = offset as i64;
                if end_offset < o {
                    eprintln!("Argument end-offset must be greater than or equal to specified offset");
                }
            } else {
                if end_offset < 0 {
                    eprintln!("Argument end-offset must be greater than or equal to zero");
                } else {
                    builder.end_offset(Some(end_offset));
                }
            }
        }

        let consume_config = builder.build()?;
        debug!("consume config: {:#?}", consume_config);

        self.consume_records_stream(consumer, offset, consume_config, tableformat)
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
        tableformat: Option<TableFormatSpec>,
    ) -> Result<()> {
        self.print_status();
        let maybe_potential_offset: Option<i64> = config.end_offset;
        let mut stream = consumer.stream_with_config(offset, config).await?;

        let templates = match self.format.as_deref() {
            None => None,
            Some(format) => {
                let mut reg = Handlebars::new();
                reg.register_template_string(USER_TEMPLATE, format)?;
                Some(reg)
            }
        };

        // TableModel and Terminal for full_table rendering
        let mut maybe_table_model = None;
        let mut maybe_terminal_stdout = if let Some(ConsumeOutputType::full_table) = &self.output {
            if io::stdout().is_tty() {
                enable_raw_mode()?;
                let mut stdout = io::stdout();
                execute!(stdout, EnterAlternateScreen)?;

                let mut model = TableModel::new();

                // Customize display options w/ tableformat spec
                model.with_tableformat(tableformat);

                maybe_table_model = Some(model);

                let stdout = io::stdout();
                Some(self.create_terminal(stdout)?)
            } else {
                None
            }
        } else {
            None
        };

        // This is used by table output, to manage printing the table titles only one time
        let mut header_print = true;

        // Below is code duplication that was needed to help CI pass
        // Without TTY, we panic when attempting to read from EventStream
        // In CI, we do not have a TTY, so we need this check to avoid reading EventStream
        // EventStream is only used by Tui+Crossterm to interact with table
        if io::stdout().is_tty() {
            // This needs to know if it is a tty before opening this
            let mut user_input_reader = EventStream::new();

            loop {
                select! {
                    stream_next = stream.next().fuse() => match stream_next {
                        Some(result) => {
                            let result: std::result::Result<Record, _> = result;
                            let record = match result {
                                Ok(record) => record,
                                /*
                                Err(FluvioError::AdminApi(ApiError::Code(code, _))) => {
                                    eprintln!("{}", code.to_sentence());
                                    continue;
                                }
                                */
                                Err(other) => return Err(other.into()),
                            };

                            self.print_record(
                                templates.as_ref(),
                                &record,
                                &mut header_print,
                                &mut maybe_terminal_stdout,
                                &mut maybe_table_model,
                            );

                            if let Some(potential_offset) = maybe_potential_offset {
                                if record.offset >= potential_offset {
                                    eprintln!("End-offset has been reached; exiting");
                                    break;
                                }
                            }
                        },
                        None => break,
                    },
                    maybe_event = user_input_reader.next().fuse() => {
                        match maybe_event {
                            Some(Ok(event)) => {
                                if let Some(model) = maybe_table_model.as_mut() {
                                    // Give the event handler access to redraw
                                    if let Some(term) = maybe_terminal_stdout.as_mut() {
                                        match model.event_handler(event, term) {
                                            TableEventResponse::Terminate => break,
                                            _ => continue
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => println!("Error: {:?}\r", e),
                            None => break,
                        }
                    },
                }
            }
        } else {
            // We do not support `--output=full_table` when we don't have a TTY (i.e., CI environment)
            while let Some(result) = stream.next().await {
                let result: std::result::Result<Record, _> = result;
                let record = match result {
                    Ok(record) => record,
                    /*
                    Err(FluvioError::AdminApi(ApiError::Code(code, _))) => {
                        eprintln!("{}", code.to_sentence());
                        continue;
                    }
                    */
                    Err(other) => return Err(other.into()),
                };

                self.print_record(
                    templates.as_ref(),
                    &record,
                    &mut header_print,
                    &mut None,
                    &mut None,
                );
            }
        }

        if let Some(ConsumeOutputType::full_table) = &self.output {
            if let Some(mut terminal_stdout) = maybe_terminal_stdout {
                disable_raw_mode()?;
                execute!(terminal_stdout.backend_mut(), LeaveAlternateScreen,)?;
                terminal_stdout.show_cursor()?;
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
        table_model: &mut Option<TableModel>,
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

                value
            }
            (Some(ConsumeOutputType::full_table), None) => {
                if let Some(ref mut table) = table_model {
                    format_fancy_table_record(record.value(), table)
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
        } else if let Some(term) = terminal {
            if let Some(table) = table_model {
                table.render(term);
            }
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

fn create_smartmodule(
    name_or_path: &str,
    kind: SmartModuleKind,
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
