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

    use std::time::{UNIX_EPOCH, Duration};
    use std::{io::Error as IoError, path::PathBuf};
    use std::io::{self, ErrorKind, Stdout};
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::sync::Arc;

    use tracing::{debug, trace, instrument};
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
    use anyhow::Result;

    use fluvio_types::PartitionId;
    use fluvio_spu_schema::server::smartmodule::SmartModuleContextData;
    use fluvio_protocol::record::NO_TIMESTAMP;
    use fluvio::metadata::tableformat::TableFormatSpec;
    use fluvio_future::io::StreamExt;
    use fluvio::{ConsumerConfig, Fluvio, MultiplePartitionConsumer, Offset, FluvioError};
    use fluvio::consumer::{PartitionSelectionStrategy, Record};
    use fluvio_spu_schema::Isolation;

    use crate::monitoring::init_monitoring;
    use crate::render::ProgressRenderer;
    use crate::CliError;
    use crate::common::FluvioExtensionMetadata;
    use crate::util::{parse_isolation, parse_key_val};
    use crate::common::Terminal;
    use crate::client::smartmodule_invocation::{
        create_smartmodule, create_smartmodule_from_path, create_smartmodule_list,
    };

    use super::record_format::{
        format_text_record, format_binary_record, format_dynamic_record, format_raw_record,
        format_json, format_basic_table_record, format_fancy_table_record,
    };
    use super::super::ClientCmd;
    use super::table_format::{TableEventResponse, TableModel};
    use fluvio_smartengine::transformation::TransformationConfig;

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
        pub partition: PartitionId,

        /// Consume records from all partitions
        #[clap(short = 'A', long = "all-partitions", conflicts_with_all = &["partition"])]
        pub all_partitions: bool,

        /// Disable continuous processing of messages
        #[clap(short = 'd', long)]
        pub disable_continuous: bool,

        /// Disable the progress bar and wait spinner
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

        /// Consume records from the beginning of the log
        #[clap(short = 'B', long,  conflicts_with_all = &["head","start", "tail"])]
        pub beginning: bool,

        /// Consume records starting <integer> from the beginning of the log
        #[clap(short = 'H', long, value_name = "integer", conflicts_with_all = &["beginning", "start", "tail"])]
        pub head: Option<u32>,

        /// Consume records starting <integer> from the end of the log
        #[clap(short = 'T', long,  value_name = "integer", conflicts_with_all = &["beginning","head", "start"])]
        pub tail: Option<u32>,

        /// The absolute offset of the first record to begin consuming from
        #[clap(long, value_name = "integer", conflicts_with_all = &["beginning", "head", "tail"])]
        pub start: Option<u32>,

        /// Consume records until end offset (inclusive)
        #[clap(long, value_name = "integer")]
        pub end: Option<u32>,

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

        /// Name of the smartmodule
        #[clap(
            long,
            group("smartmodule_group"),
            group("aggregate_group"),
            alias = "sm"
        )]
        pub smartmodule: Option<String>,

        /// Path to the smart module
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
        /// Eg. fluvio consume topic-name --smartmodule my_filter -e foo=bar -e key=value -e one=1
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

        /// (Optional) Path to a file with transformation specification.
        #[clap(long, conflicts_with = "smartmodule_group")]
        pub transforms_file: Option<PathBuf>,

        /// (Optional) Transformation specification as JSON formatted string.
        /// E.g. fluvio consume topic-name --transform='{"uses":"infinyon/jolt@0.1.0","with":{"spec":"[{\"operation\":\"default\",\"spec\":{\"source\":\"test\"}}]"}}'
        #[clap(long, short, conflicts_with_all = &["smartmodule_group", "transforms_file"])]
        pub transform: Vec<String>,
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
                        return Err(
                            CliError::TableFormatNotFound(tableformat_name.to_string()).into()
                        );
                    }

                    found
                } else {
                    return Err(CliError::TableFormatNotFound(tableformat_name.to_string()).into());
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
            } else if !self.transform.is_empty() {
                let config =
                    TransformationConfig::try_from(self.transform.clone()).map_err(|err| {
                        CliError::InvalidArg(format!("unable to parse `transform` argument: {err}"))
                    })?;
                create_smartmodule_list(config)?
            } else if let Some(transforms_file) = &self.transforms_file {
                let config = TransformationConfig::from_file(transforms_file).map_err(|err| {
                    CliError::InvalidArg(format!(
                        "unable to process `transforms_file` argument: {err}"
                    ))
                })?;
                create_smartmodule_list(config)?
            } else {
                Vec::new()
            };

            builder.smartmodule(smart_module);

            if self.disable_continuous {
                builder.disable_continuous(true);
            }

            if let Some(end_offset) = self.end {
                if let Some(start_offset) = self.start {
                    if end_offset < start_offset {
                        eprintln!(
                            "Argument end-offset must be greater than or equal to specified start offset"
                        );
                        return Err(CliError::from(FluvioError::CrossingOffsets(
                            start_offset,
                            end_offset,
                        ))
                        .into());
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
            let maybe_potential_end_offset: Option<u32> = self.end;
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

                                if let Some(potential_offset) = maybe_potential_end_offset {
                                    if record.offset >= potential_offset as i64 {
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
                                Some(Err(e)) => println!("Error: {e:?}\r"),
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

                    if let Some(potential_offset) = maybe_potential_end_offset {
                        if record.offset >= potential_offset as i64 {
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
                        let output = format!("[{formatted_key}] {value}");
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

        fn format_status_string(&self) -> String {
            let prefix = format!("Consuming records from '{}'", self.topic);
            let starting_description = if self.beginning {
                " starting from the beginning of log".to_string()
            } else if let Some(offset) = self.head {
                format!(" starting {offset} from the beginning of log")
            } else if let Some(offset) = self.start {
                format!(" starting at offset {offset}")
            } else if let Some(offset) = self.tail {
                format!(" starting {offset} from the end of log")
            } else {
                "".to_string()
            };

            let ending_description = if let Some(end) = self.end {
                format!(" until offset {end} (inclusive)")
            } else {
                "".to_string()
            };

            format!("{prefix}{starting_description}{ending_description}")
        }

        fn print_status(&self) {
            use colored::*;
            if !atty::is(atty::Stream::Stdout) {
                return;
            }

            eprintln!("{}", self.format_status_string().bold());
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
                    format!("CTRL-C handler can't be initialized {err}"),
                )
                .into());
            }
            Ok(())
        }

        /// Calculate the Offset to use with the consumer based on the provided offset number
        fn calculate_offset(&self) -> Result<Offset> {
            let offset = if self.beginning {
                Offset::from_beginning(0)
            } else if let Some(offset) = self.head {
                Offset::from_beginning(offset)
            } else if let Some(offset) = self.start {
                Offset::absolute(offset as i64).unwrap()
            } else if let Some(offset) = self.tail {
                Offset::from_end(offset)
            } else {
                Offset::end()
            };

            Ok(offset)
        }
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
    #[cfg(test)]
    mod tests {
        use fluvio::Offset;

        use super::ConsumeOpt;

        fn get_opt() -> ConsumeOpt {
            ConsumeOpt {
                topic: "TOPIC_NAME".to_string(),
                partition: Default::default(),
                all_partitions: Default::default(),
                disable_continuous: Default::default(),
                disable_progressbar: Default::default(),
                key_value: Default::default(),
                format: Default::default(),
                table_format: Default::default(),
                start: Default::default(),
                head: Default::default(),
                tail: Default::default(),
                end: Default::default(),
                max_bytes: Default::default(),
                suppress_unknown: Default::default(),
                output: Default::default(),
                smartmodule: Default::default(),
                smartmodule_path: Default::default(),
                aggregate_initial: Default::default(),
                params: Default::default(),
                isolation: Default::default(),
                beginning: Default::default(),
                transforms_file: Default::default(),
                transform: Default::default(),
            }
        }
        #[test]
        fn test_format_status_string() {
            // Starting from options: --beginning --head --start --tail

            // --beginning
            let mut opt = get_opt();
            opt.beginning = true;
            assert_eq!(
                opt.format_status_string(),
                "Consuming records from 'TOPIC_NAME' starting from the beginning of log",
            );

            // --head
            let mut opt = get_opt();
            opt.head = Some(1);
            assert_eq!(
                opt.format_status_string(),
                "Consuming records from 'TOPIC_NAME' starting 1 from the beginning of log",
            );
            opt.end = Some(2);
            assert_eq!(
            opt.format_status_string(),
            "Consuming records from 'TOPIC_NAME' starting 1 from the beginning of log until offset 2 (inclusive)",
        );

            // --start
            let mut opt = get_opt();
            opt.start = Some(1);
            assert_eq!(
                opt.format_status_string(),
                "Consuming records from 'TOPIC_NAME' starting at offset 1",
            );
            opt.end = Some(2);
            assert_eq!(
            opt.format_status_string(),
            "Consuming records from 'TOPIC_NAME' starting at offset 1 until offset 2 (inclusive)",
        );

            // --tail
            let mut opt = get_opt();
            opt.tail = Some(1);
            assert_eq!(
                opt.format_status_string(),
                "Consuming records from 'TOPIC_NAME' starting 1 from the end of log",
            );
            opt.end = Some(2);
            assert_eq!(
            opt.format_status_string(),
            "Consuming records from 'TOPIC_NAME' starting 1 from the end of log until offset 2 (inclusive)",
        );

            // base case
            let mut opt = get_opt();
            assert_eq!(
                "Consuming records from 'TOPIC_NAME'",
                opt.format_status_string(),
            );
            opt.end = Some(2);
            assert_eq!(
                "Consuming records from 'TOPIC_NAME' until offset 2 (inclusive)",
                opt.format_status_string(),
            );
        }

        #[test]
        fn test_calculate_offset() {
            // default
            let opt = get_opt();
            let offset = opt.calculate_offset().unwrap();
            assert_eq!(offset, Offset::end());

            // --beginning
            let mut opt = get_opt();
            opt.beginning = true;
            let offset = opt.calculate_offset().unwrap();
            assert_eq!(offset, Offset::from_beginning(0));

            // --head
            let mut opt = get_opt();
            opt.head = Some(1);
            let offset = opt.calculate_offset().unwrap();
            assert_eq!(offset, Offset::from_beginning(1));

            // --tail
            let mut opt = get_opt();
            opt.tail = Some(1);
            let offset = opt.calculate_offset().unwrap();
            assert_eq!(offset, Offset::from_end(1));

            // --start
            let mut opt = get_opt();
            opt.start = Some(1);
            let offset = opt.calculate_offset().unwrap();
            assert_eq!(offset, Offset::absolute(1).unwrap());
        }
    }
}
