#[cfg(feature = "stats")]
mod stats;

pub use cmd::ProduceOpt;

mod cmd {

    use std::sync::Arc;
    use std::fs::File;
    use std::io::{BufReader, BufRead};
    use std::fmt::Debug;
    use std::path::PathBuf;
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::future::join_all;
    use clap::Parser;
    use tracing::{error, warn};
    use humantime::parse_duration;

    use fluvio::{
        Compression, Fluvio, FluvioError, TopicProducer, TopicProducerConfigBuilder, RecordKey,
        ProduceOutput, DeliverySemantic,
    };
    use fluvio_extension_common::Terminal;
    use fluvio_spu_schema::Isolation;
    use fluvio_types::print_cli_ok;

    use crate::client::cmd::ClientCmd;
    use crate::common::FluvioExtensionMetadata;
    use crate::Result;
    use crate::monitoring::init_monitoring;
    use crate::util::parse_isolation;

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

        /// Sends key/value records split on the first instance of the separator.
        #[clap(long, value_parser = validate_key_separator)]
        pub key_separator: Option<String>,

        /// Send all input as one record. Use this when producing binary files.
        #[clap(long)]
        pub raw: bool,

        /// Compression algorithm to use when sending records.
        /// Supported values: none, gzip, snappy and lz4.
        #[clap(long)]
        pub compression: Option<Compression>,

        /// Path to a file to produce to the topic. If absent, producer will read stdin.
        #[clap(short, long)]
        pub file: Option<PathBuf>,

        /// Time to wait before sending
        /// Ex: '150ms', '20s'
        #[clap(long, value_parser=parse_duration)]
        pub linger: Option<Duration>,

        /// Max amount of bytes accumulated before sending
        #[clap(long)]
        pub batch_size: Option<usize>,

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

            if self.raw {
                // Read all input and send as one record
                let buffer = match &self.file {
                    Some(path) => std::fs::read(&path)?,
                    None => {
                        let mut buffer = Vec::new();
                        std::io::Read::read_to_end(&mut std::io::stdin(), &mut buffer)?;
                        buffer
                    }
                };

                let produce_output = producer.send(RecordKey::NULL, buffer).await?;

                if self.delivery_semantic != DeliverySemantic::AtMostOnce {
                    produce_output.wait().await?;
                }

                #[cfg(feature = "stats")]
                if self.is_stats_collect() && self.is_print_live_stats() {
                    self.update_stats_bar(maybe_stats_bar.as_ref(), &producer, "");
                }
            } else {
                // Read input line-by-line and send as individual records
                #[cfg(feature = "stats")]
                self.produce_lines(producer.clone(), maybe_stats_bar.as_ref())
                    .await?;
                #[cfg(not(feature = "stats"))]
                self.produce_lines(producer.clone()).await?;
            };

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

            match &self.file {
                Some(path) => {
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
                }
                None => {
                    let mut lines = BufReader::new(std::io::stdin()).lines();
                    if self.interactive_mode() {
                        eprint!("> ");
                    }

                    while let Some(Ok(line)) = lines.next() {
                        let produce_output = self.produce_line(&producer, &line).await?;

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
                }
            };

            #[cfg(feature = "stats")]
            if let Some(file) = maybe_stats_file.as_mut() {
                file.flush()?;
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
                println!("[{}] {}", key, value);
            }

            Ok(Some(producer.send(key, value).await?))
        }

        fn interactive_mode(&self) -> bool {
            self.file.is_none() && atty::is(atty::Stream::Stdin)
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
