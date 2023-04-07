#[cfg(feature = "stats")]
mod stats;

pub use cmd::ProduceOpt;

mod cmd {

    use std::sync::Arc;
    use std::io::{BufReader, BufRead};
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::time::Duration;
    #[cfg(feature = "producer-file-io")]
    use std::fs::File;
    #[cfg(feature = "producer-file-io")]
    use std::path::PathBuf;

    use async_trait::async_trait;
    #[cfg(feature = "producer-file-io")]
    use futures::future::join_all;
    use clap::Parser;
    use tracing::{error, warn};
    use humantime::parse_duration;
    use anyhow::Result;

    use fluvio::{
        Compression, Fluvio, FluvioError, TopicProducer, TopicProducerConfigBuilder, RecordKey,
        ProduceOutput, DeliverySemantic, SmartModuleContextData, Isolation, SmartModuleInvocation,
    };
    use fluvio_extension_common::Terminal;
    use fluvio_types::print_cli_ok;

    #[cfg(feature = "producer-file-io")]
    use fluvio_cli_common::user_input::{UserInputRecords, UserInputType};
    #[cfg(feature = "producer-file-io")]
    use fluvio_protocol::record::RecordData;
    #[cfg(feature = "producer-file-io")]
    use fluvio_protocol::bytes::Bytes;

    use crate::client::cmd::ClientCmd;
    use crate::common::FluvioExtensionMetadata;
    use crate::monitoring::init_monitoring;
    use crate::util::{parse_isolation, parse_key_val};
    use crate::client::smartmodule_invocation::{create_smartmodule, create_smartmodule_list};
    #[cfg(feature = "producer-file-io")]
    use crate::client::smartmodule_invocation::create_smartmodule_from_path;
    use crate::CliError;
    use fluvio_smartengine::transformation::TransformationConfig;

    #[cfg(feature = "stats")]
    use super::stats::*;

    // -----------------------------------
    // CLI Options
    // -----------------------------------

    /// Write messages to a topic/partition
    ///
    /// When no '--file' is provided, the producer will read from 'stdin'
    /// and send each line of input as one record.
    ///
    /// If a file is given with '--file', the file is sent as one entire record.
    ///
    /// If '--key-separator' is used, records are sent as key/value pairs, and
    /// the keys are used to determine which partition the records are sent to.
    #[derive(Debug, Parser)]
    pub struct ProduceOpt {
        /// The name of the Topic to produce to
        #[clap(value_name = "topic")]
        pub topic: String,

        /// Print progress output when sending records
        #[clap(short, long)]
        pub verbose: bool,

        /// Sends key/value records with this value as key
        #[clap(long, group = "RecordKey")]
        pub key: Option<String>,

        /// Sends key/value records split on the first instance of the separator.
        #[cfg(feature = "producer-file-io")]
        #[clap(long, value_parser = validate_key_separator, group = "RecordKey", conflicts_with = "raw")]
        pub key_separator: Option<String>,
        #[cfg(not(feature = "producer-file-io"))]
        #[clap(long, value_parser = validate_key_separator, group = "RecordKey")]
        pub key_separator: Option<String>,

        #[cfg(feature = "producer-file-io")]
        /// Send all input as one record. Use this when producing binary files.
        #[clap(long)]
        pub raw: bool,

        /// Compression algorithm to use when sending records.
        /// Supported values: none, gzip, snappy and lz4.
        #[clap(long)]
        pub compression: Option<Compression>,

        #[cfg(feature = "producer-file-io")]
        /// Path to a file to produce to the topic.
        /// Default: Each line treated as single record unless `--raw` specified.
        /// If absent, producer will read stdin.
        #[clap(short, long, groups = ["TestFile"])]
        pub file: Option<PathBuf>,

        /// Time to wait before sending
        /// Ex: '150ms', '20s'
        #[clap(long, value_parser=parse_duration)]
        pub linger: Option<Duration>,

        /// Max amount of bytes accumulated before sending
        #[clap(long)]
        pub batch_size: Option<usize>,

        /// Name of the smartmodule
        #[clap(
            long,
            group("smartmodule_group"),
            group("aggregate_group"),
            alias = "sm"
        )]
        pub smartmodule: Option<String>,

        #[cfg(feature = "producer-file-io")]
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
        /// Eg. fluvio produce topic-name --smartmodule my_filter -e foo=bar -e key=value -e one=1
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

        #[cfg(feature = "producer-file-io")]
        /// (Optional) Path to a file with transformation specification.
        #[clap(long, conflicts_with = "smartmodule_group")]
        pub transforms_file: Option<PathBuf>,

        /// (Optional) Transformation specification as JSON formatted string.
        /// E.g. fluvio produce topic-name --transform='{"uses":"infinyon/jolt@0.1.0","with":{"spec":"[{\"operation\":\"default\",\"spec\":{\"source\":\"test\"}}]"}}'
        #[clap(long, short, conflicts_with_all = &["smartmodule_group", "transforms_file"])]
        pub transform: Vec<String>,

        /// Isolation level that producer must respect.
        /// Supported values: read_committed (ReadCommitted) - wait for records to be committed before response,
        /// read_uncommitted (ReadUncommitted) - just wait for leader to accept records.
        #[clap(long, value_parser=parse_isolation)]
        pub isolation: Option<Isolation>,

        /*
        #[cfg(feature = "stats")]
        /// Experimental: Collect basic producer session statistics and print in stats bar
        #[clap(long)]
        pub stats: bool,

        #[cfg(feature = "stats")]
        /// Experimental: Collect all producer session statistics and print in stats bar (Implies --stats)
        #[clap(long)]
        pub stats_plus: bool,

        #[cfg(feature = "stats")]
        /// Experimental: Save producer session stats to file. The resulting file formatted for spreadsheet, as comma-separated values
        #[clap(long)]
        pub stats_path: Option<PathBuf>,

        #[cfg(feature = "stats")]
        /// Experimental: Don't display stats bar when using `--stats` or `--stats-plus`. Use with `--stats-path`.
        #[clap(long)]
        pub no_stats_bar: bool,

        #[cfg(feature = "stats")]
        /// Experimental: Only print the stats summary. Implies `--stats` and `--no-stats-bar`
        #[clap(long)]
        pub stats_summary: bool,
        */
        /// Delivery guarantees that producer must respect. Supported values:
        /// at_most_once (AtMostOnce) - send records without waiting from response,
        /// at_least_once (AtLeastOnce) - send records and retry if error occurred.
        #[clap(long, default_value = "at-least-once")]
        pub delivery_semantic: DeliverySemantic,
    }

    fn validate_key_separator(separator: &str) -> std::result::Result<String, String> {
        if separator.is_empty() {
            Err("must be non-empty. If using '=', type it as '--key-separator \"=\"'".to_string())
        } else {
            Ok(separator.to_owned())
        }
    }

    #[async_trait]
    impl ClientCmd for ProduceOpt {
        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            _out: Arc<O>,
            fluvio: &Fluvio,
        ) -> Result<()> {
            init_monitoring(fluvio.metrics());
            let config_builder = if self.interactive_mode() {
                TopicProducerConfigBuilder::default().linger(std::time::Duration::from_millis(10))
            } else {
                Default::default()
            };

            // Compression
            let config_builder = if let Some(compression) = self.compression {
                config_builder.compression(compression)
            } else {
                config_builder
            };

            // Linger
            let config_builder = if let Some(linger) = self.linger {
                config_builder.linger(linger)
            } else {
                config_builder
            };

            // Batch size
            let config_builder = if let Some(batch_size) = self.batch_size {
                config_builder.batch_size(batch_size)
            } else {
                config_builder
            };

            // Isolation
            let config_builder = if let Some(isolation) = self.isolation {
                config_builder.isolation(isolation)
            } else {
                config_builder
            };

            #[cfg(feature = "stats")]
            // Stats
            let config_builder = match (self.stats, self.stats_summary, self.stats_plus) {
                (_, _, true) => config_builder.stats_collect(ClientStatsDataCollect::All),
                (true, _, false) | (_, true, false) => {
                    config_builder.stats_collect(ClientStatsDataCollect::Data)
                }
                _ => config_builder,
            };

            // Delivery Semantic
            if self.delivery_semantic == DeliverySemantic::AtMostOnce && self.isolation.is_some() {
                warn!("Isolation is ignored for AtMostOnce delivery semantic");
            }

            let initial_param = match &self.params {
                None => BTreeMap::default(),
                Some(params) => params.clone().into_iter().collect(),
            };

            let config_builder =
                config_builder.smartmodules(self.smartmodule_invocations(initial_param)?);

            let config = config_builder
                .delivery_semantic(self.delivery_semantic)
                .build()
                .map_err(FluvioError::from)?;

            let producer = Arc::new(
                fluvio
                    .topic_producer_with_config(&self.topic, config)
                    .await?,
            );

            #[cfg(feature = "stats")]
            let maybe_stats_bar = if io::stdout().is_tty() {
                if self.is_stats_collect() {
                    let stats_bar = if self.is_print_live_stats() {
                        let s = indicatif::ProgressBar::with_draw_target(
                            Some(100),
                            indicatif::ProgressDrawTarget::stderr(),
                        );
                        s.set_style(indicatif::ProgressStyle::default_bar().template("{msg}")?);
                        Some(s)
                    } else {
                        None
                    };

                    // Handle ctrl+c to print summary stats
                    init_ctrlc(producer.clone(), stats_bar.clone(), self.stats_summary).await?;

                    stats_bar
                } else {
                    None
                }
            } else {
                // No tty
                None
            };

            #[cfg(feature = "producer-file-io")]
            if self.raw {
                self.process_raw_file(&producer).await?;
            } else {
                // Read input line-by-line and send as individual records
                #[cfg(feature = "stats")]
                self.produce_lines(producer.clone(), maybe_stats_bar.as_ref())
                    .await?;
                #[cfg(not(feature = "stats"))]
                self.produce_lines(producer.clone()).await?;
            };

            #[cfg(not(feature = "producer-file-io"))]
            {
                #[cfg(feature = "stats")]
                self.produce_lines(producer.clone(), maybe_stats_bar.as_ref())
                    .await?;
                #[cfg(not(feature = "stats"))]
                self.produce_lines(producer.clone()).await?;
            }

            producer.flush().await?;

            #[cfg(feature = "stats")]
            if self.is_stats_collect() {
                producer_summary(&producer, maybe_stats_bar.as_ref(), self.stats_summary).await;
            }

            if self.interactive_mode() {
                print_cli_ok!();
            }

            Ok(())
        }
    }

    impl ProduceOpt {
        #[cfg(feature = "producer-file-io")]
        async fn process_raw_file(&self, producer: &TopicProducer) -> Result<()> {
            let key = self.key.clone().map(Bytes::from);
            // Read all input and send as one record
            let buffer = match &self.file {
                Some(path) => UserInputRecords::try_from(UserInputType::File {
                    key: key.clone(),
                    path: path.to_path_buf(),
                })
                .unwrap_or_default(),

                None => {
                    let mut buffer = Vec::new();
                    std::io::Read::read_to_end(&mut std::io::stdin(), &mut buffer)?;
                    UserInputRecords::try_from(UserInputType::Text {
                        key: key.clone(),
                        data: Bytes::from(buffer),
                    })
                    .unwrap_or_default()
                }
            };

            let key = if let Some(key) = buffer.key() {
                RecordKey::from(key)
            } else {
                RecordKey::NULL
            };

            let data: RecordData = buffer.into();

            let produce_output = producer.send(key, data).await?;

            if self.delivery_semantic != DeliverySemantic::AtMostOnce {
                produce_output.wait().await?;
            }

            #[cfg(feature = "stats")]
            if self.is_stats_collect() && self.is_print_live_stats() {
                self.update_stats_bar(maybe_stats_bar.as_ref(), &producer, "");
            }

            Ok(())
        }

        pub fn smart_module_ctx(&self) -> SmartModuleContextData {
            if let Some(agg_initial) = &self.aggregate_initial {
                SmartModuleContextData::Aggregate {
                    accumulator: agg_initial.clone().into_bytes(),
                }
            } else {
                SmartModuleContextData::None
            }
        }

        async fn produce_lines(
            &self,
            producer: Arc<TopicProducer>,
            #[cfg(feature = "stats")] maybe_stats_bar: Option<&ProgressBar>,
        ) -> Result<()> {
            #[cfg(feature = "stats")]
            // If stats file
            let mut maybe_stats_file = if self.is_stats_collect() {
                if let Some(stats_path) = &self.stats_path {
                    let stats_file = start_csv_report(stats_path, &producer).await?;
                    Some(stats_file)
                } else {
                    None
                }
            } else {
                None
            };

            #[cfg(feature = "stats")]
            // Avoid writing duplicate data to disk
            let mut stats_dataframe_check = 0;

            #[cfg(feature = "producer-file-io")]
            if let Some(path) = &self.file {
                let reader = BufReader::new(File::open(path)?);
                let mut produce_outputs = vec![];
                for line in reader.lines().filter_map(|it| it.ok()) {
                    let produce_output = self.produce_line(&producer, &line).await?;

                    if let Some(produce_output) = produce_output {
                        produce_outputs.push(produce_output);
                    }

                    #[cfg(feature = "stats")]
                    if self.is_stats_collect() {
                        if self.is_print_live_stats() {
                            self.update_stats_bar(maybe_stats_bar, &producer, &line);
                        }

                        stats_dataframe_check = write_csv_dataframe(
                            &producer,
                            stats_dataframe_check,
                            maybe_stats_file.as_mut(),
                        )
                        .await?;
                    }
                }

                if self.delivery_semantic != DeliverySemantic::AtMostOnce {
                    // ensure all records were properly sent
                    join_all(
                        produce_outputs
                            .into_iter()
                            .map(|produce_output| produce_output.wait()),
                    )
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;
                }
            } else {
                self.producer_stdin(&producer).await?
            }

            #[cfg(not(feature = "producer-file-io"))]
            self.producer_stdin(&producer).await?;

            #[cfg(feature = "stats")]
            if let Some(file) = maybe_stats_file.as_mut() {
                file.flush()?;
            }

            Ok(())
        }

        async fn producer_stdin(&self, producer: &Arc<TopicProducer>) -> Result<()> {
            let mut lines = BufReader::new(std::io::stdin()).lines();
            if self.interactive_mode() {
                eprint!("> ");
            }

            while let Some(Ok(line)) = lines.next() {
                let produce_output = self.produce_line(producer, &line).await?;

                if let Some(produce_output) = produce_output {
                    if self.delivery_semantic != DeliverySemantic::AtMostOnce {
                        // ensure it was properly sent
                        produce_output.wait().await?;
                    }
                }

                #[cfg(feature = "stats")]
                if self.is_stats_collect() {
                    if self.is_print_live_stats() {
                        self.update_stats_bar(maybe_stats_bar, &producer, &line);
                    }

                    stats_dataframe_check = write_csv_dataframe(
                        &producer,
                        stats_dataframe_check,
                        maybe_stats_file.as_mut(),
                    )
                    .await?;
                }

                if self.interactive_mode() {
                    #[cfg(feature = "stats")]
                    if let Some(file) = maybe_stats_file.as_mut() {
                        file.flush()?;
                    }

                    print_cli_ok!();
                    eprint!("> ");
                }
            }
            Ok(())
        }

        async fn produce_line(
            &self,
            producer: &Arc<TopicProducer>,
            line: &str,
        ) -> Result<Option<ProduceOutput>> {
            let produce_output = if let Some(separator) = &self.key_separator {
                self.produce_key_value(producer.clone(), line, separator)
                    .await?
            } else if let Some(key) = &self.key {
                Some(producer.send(RecordKey::from(key.as_bytes()), line).await?)
            } else {
                Some(producer.send(RecordKey::NULL, line).await?)
            };

            Ok(produce_output)
        }

        async fn produce_key_value(
            &self,
            producer: Arc<TopicProducer>,
            line: &str,
            separator: &str,
        ) -> Result<Option<ProduceOutput>> {
            let maybe_kv = line.split_once(separator);
            let (key, value) = match maybe_kv {
                Some(kv) => kv,
                None => {
                    error!(
                        "Failed to find separator '{}' in record, skipping: '{}'",
                        separator, line
                    );
                    return Ok(None);
                }
            };

            if self.verbose {
                println!("[{key}] {value}");
            }

            Ok(Some(producer.send(key, value).await?))
        }

        #[cfg(feature = "producer-file-io")]
        fn interactive_mode(&self) -> bool {
            self.file.is_none() && atty::is(atty::Stream::Stdin)
        }

        #[cfg(not(feature = "producer-file-io"))]
        fn interactive_mode(&self) -> bool {
            atty::is(atty::Stream::Stdin)
        }

        #[cfg(feature = "stats")]
        fn is_stats_collect(&self) -> bool {
            self.stats || self.stats_plus || self.stats_summary
        }

        #[cfg(feature = "stats")]
        fn is_print_live_stats(&self) -> bool {
            !self.no_stats_bar && !self.stats_summary
        }

        #[cfg(feature = "stats")]
        fn update_stats_bar(
            &self,
            maybe_stats_bar: Option<&ProgressBar>,
            producer: &Arc<TopicProducer>,
            line: &str,
        ) {
            if self.is_print_live_stats() {
                if let (Some(stats_bar), Some(producer_stats)) = (maybe_stats_bar, producer.stats())
                {
                    stats_bar.set_message(format_current_stats(producer_stats));

                    if self.interactive_mode() {
                        stats_bar.println(line);
                    }
                }
            }
        }

        pub fn metadata() -> FluvioExtensionMetadata {
            FluvioExtensionMetadata {
                title: "produce".into(),
                package: Some("fluvio/fluvio".parse().unwrap()),
                description: "Produce new data in a stream".into(),
                version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
            }
        }

        fn smartmodule_invocations(
            &self,
            initial_param: BTreeMap<String, String>,
        ) -> Result<Vec<SmartModuleInvocation>> {
            if let Some(smart_module_name) = &self.smartmodule {
                return Ok(vec![create_smartmodule(
                    smart_module_name,
                    self.smart_module_ctx(),
                    initial_param,
                )]);
            }

            #[cfg(feature = "producer-file-io")]
            if let Some(path) = &self.smartmodule_path {
                return Ok(vec![create_smartmodule_from_path(
                    path,
                    self.smart_module_ctx(),
                    initial_param,
                )?]);
            }

            if !self.transform.is_empty() {
                let config =
                    TransformationConfig::try_from(self.transform.clone()).map_err(|err| {
                        CliError::InvalidArg(format!("unable to parse `transform` argument: {err}"))
                    })?;
                return create_smartmodule_list(config);
            }

            #[cfg(feature = "producer-file-io")]
            if let Some(transforms_file) = &self.transforms_file {
                let config = TransformationConfig::from_file(transforms_file).map_err(|err| {
                    CliError::InvalidArg(format!(
                        "unable to process `transforms_file` argument: {err}"
                    ))
                })?;

                return create_smartmodule_list(config);
            }

            Ok(Vec::new())
        }
    }

    #[cfg(feature = "stats")]
    /// Initialize Ctrl-C event handler to print session summary when we are collecting stats
    async fn init_ctrlc(
        producer: Arc<TopicProducer>,
        maybe_stats_bar: Option<ProgressBar>,
        force_print_summary: bool,
    ) -> Result<()> {
        let result = ctrlc::set_handler(move || {
            fluvio_future::task::run_block_on(async {
                producer_summary(&producer, maybe_stats_bar.as_ref(), force_print_summary).await;
            });

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
}
