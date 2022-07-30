//!
//! # Consume CLI
//!
//! CLI command for Consume operation
//!
//! 
//! mod record_format;
mod table_format;
mod record_format;

use table_format::TableModel;
pub use cmd::ConsumeOpt;

mod cmd {


    use std::time::UNIX_EPOCH;
    use std::{io::Error as IoError, path::PathBuf};
    use std::io::{self, ErrorKind, Read, Stdout};
    use std::collections::{BTreeMap};

    use flate2::Compression;
    use flate2::bufread::GzEncoder;
    use handlebars::{self, Handlebars};
    use tracing::{debug, trace, instrument};
    use clap::{Parser, ArgEnum};
    use futures::{select, FutureExt};

    use fluvio::dataplane::batch::NO_TIMESTAMP;
    use fluvio::metadata::tableformat::{TableFormatSpec};
    use fluvio_spu_schema::server::stream_fetch::SmartModuleContextData;
    use fluvio_future::io::StreamExt;
    

    use super::table_format::{TableEventResponse, TableModel};

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

    use crate::render::ProgressRenderer;
    use crate::{CliError, Result};
    use crate::common::FluvioExtensionMetadata;
    use crate::parse_isolation;
    
    use super::record_format::{
        format_text_record, format_binary_record, format_dynamic_record, format_raw_record,
        format_json, format_basic_table_record, format_fancy_table_record,
    };

    use fluvio::dataplane::Isolation;

    const DEFAULT_TAIL: u32 = 10;
    const USER_TEMPLATE: &str = "user_template";

    /// Read messages from a topic/partition
    ///
    /// By default, consume operates in "streaming" mode, where the command will remain
    /// active and wait for new messages, printing them as they arrive. You can use the
    /// '-d' flag to exit after consuming all available messages.
    #[derive(Debug, Parser)]
    pub struct ConsumeOpt {
        /// Topic name
        #[clap(value_name = "topic")]
        pub topic: String,

        /// Partition id
        #[clap(short = 'p', long, default_value = "0", value_name = "integer")]
        pub partition: i32,

        /// Consume records from all partitions
        #[clap(short = 'A', long = "all-partitions", conflicts_with_all = &["partition"])]
        pub all_partitions: bool,

        /// disable continuous processing of messages
        #[clap(short = 'd', long)]
        pub disable_continuous: bool,

        /// disable the progress bar and wait spinner
        #[clap(long)]
        pub disable_progressbar: bool,

        /// Print records in "[key] value" format, with "[null]" for no key
        #[clap(short, long)]
        pub key_value: bool,

        /// Provide a template string to print records with a custom format.
        /// See --help for details.
        ///
        /// Template strings may include the variables {{key}}, {{value}}, {{offset}}, {{partition}} and {{time}}
        /// which will have each record's contents substituted in their place.
        /// Note that timestamp is displayed using RFC3339, is always UTC and ignores system timezone.
        ///
        /// For example, the following template string:
        ///
        /// Offset {{offset}} has key {{key}} and value {{value}}
        ///
        /// Would produce a printout where records might look like this:
        ///
        /// Offset 0 has key A and value Apple
        #[clap(short = 'F', long, conflicts_with_all = &["output", "key-value"])]
        pub format: Option<String>,

        /// Consume records using the formatting rules defined by TableFormat name
        #[clap(long, conflicts_with_all = &["key-value", "format"])]
        pub table_format: Option<String>,

        /// Consume records starting X from the beginning of the log (default: 0)
        #[clap(short = 'B', value_name = "integer", conflicts_with_all = &["offset", "tail"])]
        pub from_beginning: Option<Option<u32>>,

        /// The offset of the first record to begin consuming from
        #[clap(short, long, value_name = "integer", conflicts_with_all = &["from-beginning", "tail"])]
        pub offset: Option<u32>,

        /// Consume records starting X from the end of the log (default: 10)
        #[clap(short = 'T', long, value_name = "integer", conflicts_with_all = &["from-beginning", "offset"])]
        pub tail: Option<Option<u32>>,

        /// Consume records until end offset
        #[clap(long, value_name= "integer", conflicts_with_all = &["tail"])]
        pub end_offset: Option<i64>,

        /// Maximum number of bytes to be retrieved
        #[clap(short = 'b', long = "maxbytes", value_name = "integer")]
        pub max_bytes: Option<i32>,

        /// Suppress items items that have an unknown output type
        #[clap(long = "suppress-unknown")]
        pub suppress_unknown: bool,

        /// Output
        #[clap(
            short = 'O',
            long = "output",
            value_name = "type",
            arg_enum,
            ignore_case = true
        )]
        pub output: Option<ConsumeOutputType>,

        /// Name of DerivedStream
        #[clap(long)]
        pub derived_stream: Option<String>,

        /// Path to a SmartModule filter wasm file
        #[clap(long, group("smartmodule_group"))]
        pub filter: Option<String>,

        /// Path to a SmartModule map wasm file
        #[clap(long, group("smartmodule_group"))]
        pub map: Option<String>,

        /// Path to a SmartModule filter_map wasm file
        #[clap(long, group("smartmodule_group"))]
        pub filter_map: Option<String>,

        /// Path to a SmartModule array_map wasm file
        #[clap(long, group("smartmodule_group"))]
        pub array_map: Option<String>,

        /// Path to a SmartModule join wasm filee
        #[clap(long, group("smartmodule_group"), group("join_group"))]
        pub join: Option<String>,

        /// Path to a WASM file for aggregation
        #[clap(long, group("smartmodule_group"), group("aggregate_group"))]
        pub aggregate: Option<String>,

        /// Path or name to WASM module. This support any of the other
        /// smartmodule types: filter, map, array_map, aggregate, join and filter_map
        #[clap(
            long,
            group("smartmodule_group"),
            group("aggregate_group"),
            group("join_group"),
            alias = "smartmodule"
        )]
        pub smart_module: Option<String>,

        #[clap(long, requires = "join_group")]
        pub join_topic: Option<String>,

        /// (Optional) Path to a file to use as an initial accumulator value with --aggregate
        #[clap(long, requires = "aggregate_group")]
        pub initial: Option<String>,

        /// (Optional) Extra input parameters passed to the smartmodule module.
        /// They should be passed using key=value format
        /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1
        #[clap(
            short = 'e',
            requires = "smartmodule_group",
            long= "extra-params",
            parse(try_from_str = parse_key_val),
            number_of_values = 1
        )]
        pub extra_params: Option<Vec<(String, String)>>,

        /// Isolation level that consumer must respect.
        /// Supported values: read_committed (ReadCommitted) - consume only committed records,
        /// read_uncommitted (ReadUncommitted) - consume all records accepted by leader.
        #[clap(long, parse(try_from_str = parse_isolation))]
        pub isolation: Option<Isolation>,
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
            let maybe_tableformat = if let Some(ref tableformat_name) = self.table_format {
                let admin = fluvio.admin().await;
                let tableformats = admin.list::<TableFormatSpec, _>(vec![]).await?;

                let mut found = None;

                if !tableformats.is_empty() {
                    for t in tableformats {
                        if t.name.as_str() == tableformat_name {
                            //println!("debug: Found tableformat: {:?}", t.spec);
                            found = Some(t.spec);
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
                self.derived_stream
                    .as_ref()
                    .map(|derivedstream_name| DerivedStreamInvocation {
                        stream: derivedstream_name.clone(),
                        params: extra_params.clone().into(),
                    });

            builder.derivedstream(derivedstream);

            let smartmodule = if let Some(name_or_path) = &self.smart_module {
                let context = if let Some(acc_path) = &self.initial {
                    let accumulator = std::fs::read(acc_path)?;
                    SmartModuleContextData::Aggregate { accumulator }
                } else if self.join_topic.is_some() {
                    SmartModuleContextData::Join(self.join_topic.as_ref().unwrap().clone())
                } else {
                    SmartModuleContextData::None
                };
                Some(create_smartmodule(
                    name_or_path,
                    SmartModuleKind::Generic(context),
                    extra_params,
                )?)
            } else if let Some(name_or_path) = &self.filter {
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
                if end_offset < 0 {
                    eprintln!("Argument end-offset must be greater than or equal to zero");
                }
                if let Some(offset) = self.offset {
                    let o = offset as i64;
                    if end_offset < o {
                        eprintln!(
                            "Argument end-offset must be greater than or equal to specified offset"
                        );
                    }
                }
            }

            if let Some(isolation) = self.isolation {
                builder.isolation(isolation);
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
            let maybe_potential_offset: Option<i64> = self.end_offset;
            let mut stream = consumer.stream_with_config(offset, config).await?;

            let templates = match self.format.as_deref() {
                None => None,
                Some(format) => {
                    let mut reg = Handlebars::new();
                    // opt-out of HTML escaping of printable record data
                    reg.register_escape_fn(handlebars::no_escape);
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
                let pb = indicatif::ProgressBar::new(1);

                // Prevent the progress bars from displaying if we're using full_table
                // or if we've explicitly disabled it
                if let Some(ConsumeOutputType::full_table) = &self.output {
                    // Do nothing.
                } else if !self.disable_progressbar {
                    pb.set_style(indicatif::ProgressStyle::default_bar().template("{spinner}"));
                    pb.enable_steady_tick(100);
                }

                let pb: ProgressRenderer = pb.into();

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
                                    &pb,
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
                let pb = ProgressRenderer::default();
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
                        &pb,
                    );

                    if let Some(potential_offset) = maybe_potential_offset {
                        if record.offset >= potential_offset {
                            eprintln!("End-offset has been reached; exiting");
                            break;
                        }
                    }
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
            pb: &ProgressRenderer,
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
                    let timestamp_rfc3339 = if record.timestamp() == NO_TIMESTAMP {
                        "NA".to_string()
                    } else {
                        format!(
                            "{}",
                            humantime::format_rfc3339_millis(
                                UNIX_EPOCH
                                    + std::time::Duration::from_millis(
                                        record.timestamp().try_into().unwrap_or_default()
                                    )
                            )
                        )
                    };

                    let object = serde_json::json!({
                        "key": formatted_key,
                        "value": value,
                        "offset": record.offset(),
                        "partition": record.partition(),
                        "time": timestamp_rfc3339,
                    });
                    templates.render(USER_TEMPLATE, &object).ok()
                }
            };

            // If the consume type is table, we don't want to accidentally print a newline
            if self.output != Some(ConsumeOutputType::full_table) {
                match formatted_value {
                    Some(value) if self.key_value => {
                        let output = format!("[{}] {}", formatted_key, value);
                        pb.println(&output);
                    }
                    Some(value) => {
                        pb.println(&value);
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
            // If --end-offset=X
            } else if let Some(end) = self.end_offset {
                eprintln!(
                    "{}",
                    format!(
                        "Consuming records starting from the end until record {} in topic '{}'",
                        end, &self.topic
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
        let wasm = if PathBuf::from(name_or_path).is_file() {
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

    // Uses clap::ArgEnum to choose possible variables

    #[derive(ArgEnum, Debug, Clone, PartialEq)]
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

    /// Consume output type defaults to text formatting
    impl ::std::default::Default for ConsumeOutputType {
        fn default() -> Self {
            ConsumeOutputType::dynamic
        }
    }
}