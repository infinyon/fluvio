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

    use std::path::Path;
    use std::time::{UNIX_EPOCH, Duration};
    use std::{io::Error as IoError, path::PathBuf};
    use std::io::{self, ErrorKind, Read, Stdout};
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::sync::Arc;

    use tracing::{debug, trace, instrument};
    use flate2::Compression;
    use flate2::bufread::GzEncoder;
    use clap::{Parser, ValueEnum};
    use futures::{select, FutureExt};
    use async_trait::async_trait;
    use tui::Terminal as TuiTerminal;
    use tui::backend::CrosstermBackend;
    use crossterm::tty::IsTty;
    use crossterm::{
        event::EventStream,
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    };
    use handlebars::{self, Handlebars};

    use fluvio_spu_schema::server::smartmodule::{
        SmartModuleContextData, SmartModuleKind, SmartModuleInvocation, SmartModuleInvocationWasm,
    };
    use fluvio_protocol::record::NO_TIMESTAMP;
    use fluvio::metadata::tableformat::{TableFormatSpec};
    use fluvio_future::io::StreamExt;
    use fluvio::{ConsumerConfig, Fluvio, MultiplePartitionConsumer, Offset};
    use fluvio::consumer::{PartitionSelectionStrategy, Record};
    use fluvio_spu_schema::Isolation;

    use crate::monitoring::init_monitoring;
    use crate::render::ProgressRenderer;
    use crate::{CliError, Result};
    use crate::common::FluvioExtensionMetadata;
    use crate::util::parse_isolation;
    use crate::common::Terminal;

    use super::record_format::{
        format_text_record, format_binary_record, format_dynamic_record, format_raw_record,
        format_json, format_basic_table_record, format_fancy_table_record,
    };
    use super::super::ClientCmd;
    use super::table_format::{TableEventResponse, TableModel};

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
        #[clap(short = 'F', long, conflicts_with_all = &["output"])]
        pub format: Option<String>,

        /// Consume records using the formatting rules defined by TableFormat name
        #[clap(long)]
        pub table_format: Option<String>,

        /// The offset of the first record to begin consuming from
        #[clap(short, long, value_name = "integer", conflicts_with_all = &["tail"])]
        pub offset: Option<u32>,

        /// Consume records starting X from the beginning of the log (default:0, change X with -n)
        #[clap(short = 'B', long="head", group = "can_be_offset", conflicts_with_all = &["offset", "tail"])]
        pub from_beginning: bool,

        /// Consume records starting X from the end of the log (default: 10, change X with -n)
        #[clap(short = 'T', long,  group = "can_be_offset", conflicts_with_all = &["from-beginning", "offset"])]
        pub tail: bool,

        /// --head consume beginning X after start of log. --tail consume beginning X before end of log.
        #[clap(short = 'n', value_name = "integer", requires("can_be_offset"))]
        pub amount_to_offset: Option<u32>,

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
            value_enum,
            ignore_case = true
        )]
        pub output: Option<ConsumeOutputType>,

        /// name of the smart module
        #[clap(
            long,
            group("smartmodule_group"),
            group("aggregate_group"),
            alias = "sm"
        )]
        pub smartmodule: Option<String>,

        #[clap(
            long,
            group("smartmodule_group"),
            group("aggregate_group"),
            alias = "sm_path"
        )]
        pub smartmodule_path: Option<PathBuf>,

        /// (Optional) Path to a file to use as an initial accumulator value with --aggregate
        #[clap(long, requires = "aggregate_group", alias = "a-init")]
        pub aggregate_initial: Option<String>,

        /// (Optional) Extra input parameters passed to the smartmodule module.
        /// They should be passed using key=value format
        /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1
        #[clap(
            short = 'e',
            requires = "smartmodule_group",
            long="params",
            value_parser=parse_key_val,
            // value_parser,
            // action,
            number_of_values = 1
        )]
        pub params: Option<Vec<(String, String)>>,

        /// Isolation level that consumer must respect.
        /// Supported values: read_committed (ReadCommitted) - consume only committed records,
        /// read_uncommitted (ReadUncommitted) - consume all records accepted by leader.
        #[clap(long, value_parser=parse_isolation)]
        pub isolation: Option<Isolation>,
    }

    fn parse_key_val(s: &str) -> Result<(String, String)> {
        let pos = s.find('=').ok_or_else(|| {
            CliError::InvalidArg(format!("invalid KEY=value: no `=` found in `{}`", s))
        })?;
        Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
    }

    #[async_trait]
    impl ClientCmd for ConsumeOpt {
        #[instrument(
            skip(self, fluvio),
            name = "Consume",
            fields(topic = %self.topic, partition = self.partition),
        )]
        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            _out: Arc<O>,
            fluvio: &Fluvio,
        ) -> Result<()> {
            init_monitoring(fluvio.metrics());

            //println!("client id",fluvio);

            let maybe_tableformat = if let Some(ref tableformat_name) = self.table_format {
                let admin = fluvio.admin().await;
                let tableformats = admin.all::<TableFormatSpec>().await?;

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
    }

    impl ConsumeOpt {
        pub fn metadata() -> FluvioExtensionMetadata {
            FluvioExtensionMetadata {
                title: "consume".into(),
                package: Some("fluvio/fluvio".parse().unwrap()),
                description: "Consume new data in a stream".into(),
                version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
            }
        }

        fn smart_module_ctx(&self) -> SmartModuleContextData {
            if let Some(agg_initial) = &self.aggregate_initial {
                SmartModuleContextData::Aggregate {
                    accumulator: agg_initial.clone().into_bytes(),
                }
            } else {
                SmartModuleContextData::None
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

            let initial_param = match &self.params {
                None => BTreeMap::default(),
                Some(params) => params.clone().into_iter().collect(),
            };

            let smart_module = if let Some(smart_module_name) = &self.smartmodule {
                vec![create_smartmodule(
                    smart_module_name,
                    self.smart_module_ctx(),
                    initial_param,
                )]
            } else if let Some(path) = &self.smartmodule_path {
                vec![create_smartmodule_from_path(
                    path,
                    self.smart_module_ctx(),
                    initial_param,
                )?]
            } else {
                Vec::new()
            };

            builder.smartmodule(smart_module);

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
            let mut maybe_terminal_stdout =
                if let Some(ConsumeOutputType::full_table) = &self.output {
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
                    pb.set_style(indicatif::ProgressStyle::default_bar().template("{spinner}")?);
                    pb.enable_steady_tick(Duration::from_millis(100));
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

        fn create_terminal(&self, stdout: Stdout) -> Result<TuiTerminal<CrosstermBackend<Stdout>>> {
            let backend = CrosstermBackend::new(stdout);
            let terminal = TuiTerminal::new(backend)?;

            Ok(terminal)
        }

        /// Process fetch topic response based on output type
        pub fn print_record(
            &self,
            templates: Option<&Handlebars>,
            record: &Record,
            header_print: &mut bool,
            terminal: &mut Option<TuiTerminal<CrosstermBackend<Stdout>>>,
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
                (Some(ConsumeOutputType::binary), None) => {
                    Some(format_binary_record(record.value()))
                }
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

            if self.from_beginning {
                // If --from-beginning -n X
                if let Some(offset) = self.amount_to_offset {
                    eprintln!(
                        "{}",
                        format!(
                            "Consuming records starting {} from the beginning of topic '{}'",
                            offset, &self.topic
                        )
                        .bold()
                    );
                }
                // If --from-beginning=X
                else {
                    eprintln!(
                        "{}",
                        format!(
                            "Consuming records from the beginning of topic '{}'",
                            &self.topic
                        )
                        .bold()
                    );
                }
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
            // If --tail or --tail -n X
            // Joined since default tail != 0
            } else if self.tail {
                let tail = self.amount_to_offset.unwrap_or(DEFAULT_TAIL);
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
            let offset = if self.from_beginning {
                let offset = self.amount_to_offset.unwrap_or(0);
                Offset::from_beginning(offset)
            } else if let Some(offset) = self.offset {
                Offset::absolute(offset as i64).unwrap()
            } else if self.tail {
                let tail = self.amount_to_offset.unwrap_or(DEFAULT_TAIL);
                Offset::from_end(tail)
            } else {
                Offset::end()
            };

            Ok(offset)
        }
    }

    /// create smartmodule from predefined name
    fn create_smartmodule(
        name: &str,
        ctx: SmartModuleContextData,
        params: BTreeMap<String, String>,
    ) -> SmartModuleInvocation {
        SmartModuleInvocation {
            wasm: SmartModuleInvocationWasm::Predefined(name.to_string()),
            kind: SmartModuleKind::Generic(ctx),
            params: params.into(),
        }
    }

    /// create smartmodule from wasm file
    fn create_smartmodule_from_path(
        path: &Path,
        ctx: SmartModuleContextData,
        params: BTreeMap<String, String>,
    ) -> Result<SmartModuleInvocation> {
        let raw_buffer = std::fs::read(path)?;
        debug!(len = raw_buffer.len(), "read wasm bytes");
        let mut encoder = GzEncoder::new(raw_buffer.as_slice(), Compression::default());
        let mut buffer = Vec::with_capacity(raw_buffer.len());
        encoder.read_to_end(&mut buffer)?;

        Ok(SmartModuleInvocation {
            wasm: SmartModuleInvocationWasm::AdHoc(buffer),
            kind: SmartModuleKind::Generic(ctx),
            params: params.into(),
        })
    }

    // Uses clap::ArgEnum to choose possible variables

    #[derive(ValueEnum, Debug, Clone, Eq, PartialEq)]
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
