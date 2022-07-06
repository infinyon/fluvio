use std::sync::Arc;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::{io::Error as IoError, path::PathBuf};
use std::io::ErrorKind;
use futures::future::join_all;
use clap::Parser;
use indicatif::ProgressBar;
use tracing::{debug, error};
use std::time::Duration;
use humantime::parse_duration;

use fluvio::{
    Compression, Fluvio, FluvioError, TopicProducer, TopicProducerConfigBuilder, RecordKey,
    ProduceOutput,
};
use fluvio::dataplane::Isolation;
use fluvio_types::print_cli_ok;
use crate::common::FluvioExtensionMetadata;
use crate::Result;
use crate::parse_isolation;

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
    #[clap(long, validator = validate_key_separator)]
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
    #[clap(long, parse(try_from_str = parse_duration))]
    pub linger: Option<Duration>,

    /// Max amount of bytes accumulated before sending
    #[clap(long)]
    pub batch_size: Option<usize>,

    /// Isolation level that producer must respect.
    /// Supported values: read_committed (ReadCommitted) - wait for records to be committed before response,
    /// read_uncommitted (ReadUncommitted) - just wait for leader to accept records.
    #[clap(long, parse(try_from_str = parse_isolation))]
    pub isolation: Option<Isolation>,

    /// Disable stats in progress bar
    #[clap(long)]
    pub no_stats: bool,

    /// Disable progress bar
    #[clap(long)]
    pub disable_progressbar: bool,
}

fn validate_key_separator(separator: &str) -> std::result::Result<(), String> {
    if separator.is_empty() {
        return Err(
            "must be non-empty. If using '=', type it as '--key-separator \"=\"'".to_string(),
        );
    }
    Ok(())
}

impl ProduceOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
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

        let config = config_builder.build().map_err(FluvioError::from)?;
        let producer = Arc::new(
            fluvio
                .topic_producer_with_config(&self.topic, config)
                .await?,
        );

        //let pb = Arc::new(self.status_bar(producer.clone()).await);

        //let update_pb = pb.clone();
        //let update_producer = producer.clone();

        //fluvio_future::task::spawn(async move {
        //    loop {
        //        fluvio_future::timer::sleep(Duration::from_millis(25)).await;
        //        update_pb.set_message(format!("{}", update_producer.stats().await));
        //    }
        //});

        //let progress_bar_thread = fluvio_future::task::spawn(async move {
        //    let pb_2 = indicatif::ProgressBar::hidden();

        //    let m = indicatif::MultiProgress::new();

        //    let pb_1 = m.insert(0, indicatif::ProgressBar::new(100));
        //    pb_1.set_style(indicatif::ProgressStyle::default_bar().template("{spinner}"));
        //    pb_1.enable_steady_tick(1000);

        //    //let pb_2 = m.insert(1, indicatif::ProgressBar::hidden());
        //    let pb_2 = m.insert(1, indicatif::ProgressBar::new(100));
        //    pb_2.set_style(indicatif::ProgressStyle::default_bar().template("{msg}"));
        //    pb_2.set_message("Where am I?");

        //    m.set_move_cursor(false);
        //    m.join().unwrap();
        //});

        // Check on tty later
        let stats_bar =
            indicatif::ProgressBar::with_draw_target(100, indicatif::ProgressDrawTarget::stderr());
        stats_bar.set_style(indicatif::ProgressStyle::default_bar().template("{msg}"));

        self.init_ctrlc(producer.clone(), stats_bar.clone()).await?;

        // TODO: This should still print a progress bar, just in case the file is large
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

            stats_bar.set_message(producer.stats().await.print_current_stats());
            //stats_bar.tick();
            let produce_output = producer.send(RecordKey::NULL, buffer).await?;
            stats_bar.set_message(producer.stats().await.print_current_stats());
            //stats_bar.tick();

            produce_output.wait().await?;
        } else {
            // Read input line-by-line and send as individual records
            self.produce_lines(&producer, &stats_bar).await?;
            //self.produce_lines(&producer).await?;

            //let produce_fut = self.produce_lines(&producer);

            //pin_mut!(progress_bar_thread);
            //pin_mut!(produce_fut);

            //let v = match future::select(progress_bar_thread, produce_fut).await {
            //    Either::Left((_, _)) => "progress_bar", // `value1` is resolved from `future1`
            //    // `_` represents `future2`
            //    Either::Right((_, _)) => "producer", // `value2` is resolved from `future2`
            //                                         // `_` represents `future1`
            //};
            //println!("{}", v);
        }

        producer.flush().await?;
        if self.interactive_mode() {
            print_cli_ok!();
            // I should print the client stats here
            //stats_bar.set_message(producer.stats().await.print_current_stats());
            //stats_bar.set_message(producer.stats().await.print_summary_stats());
        }

        Ok(())
    }

    async fn produce_lines(&self, producer: &TopicProducer, stats_bar: &ProgressBar) -> Result<()> {
        //async fn produce_lines(&self, producer: &TopicProducer) -> Result<()> {
        match &self.file {
            Some(path) => {
                let reader = BufReader::new(File::open(path)?);
                let mut produce_outputs = vec![];
                for line in reader.lines().filter_map(|it| it.ok()) {
                    let produce_output = self.produce_line(producer, &line, stats_bar).await?;
                    //let produce_output = self.produce_line(producer, &line).await?;

                    if let Some(produce_output) = produce_output {
                        produce_outputs.push(produce_output);
                    }

                    //if self.stats {
                    //    println!("{}", producer.stats().await);
                    //}
                }

                // ensure all records were properly sent
                join_all(
                    produce_outputs
                        .into_iter()
                        .map(|produce_output| produce_output.wait()),
                )
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;

                //if self.stats {
                //    println!("{}", producer.stats().await);
                //}
            }
            None => {
                let mut lines = BufReader::new(std::io::stdin()).lines();
                if self.interactive_mode() {
                    eprint!("> ");
                    //pb.println("> ");
                }
                while let Some(Ok(line)) = lines.next() {
                    //let produce_output = self.produce_line(producer, &line, &pb).await?;
                    let produce_output = self.produce_line(producer, &line, stats_bar).await?;
                    if let Some(produce_output) = produce_output {
                        // ensure it was properly sent
                        produce_output.wait().await?;
                    }
                    if self.interactive_mode() {
                        //print_cli_ok!();
                        //if self.stats {
                        //    println!("{}", producer.stats().await);
                        //}
                        eprint!("> ");
                        //pb.println("> ");
                    } else {
                        //if self.stats {
                        //    println!("{}", producer.stats().await);
                        //}
                    }
                }
            }
        };

        Ok(())
    }

    async fn produce_line(
        &self,
        producer: &TopicProducer,
        line: &str,
        stats_bar: &ProgressBar,
    ) -> Result<Option<ProduceOutput>> {
        let produce_output = if let Some(separator) = &self.key_separator {
            self.produce_key_value(producer, line, separator).await?
        } else {
            Some(producer.send(RecordKey::NULL, line).await?)
        };

        // If we're interactive, give the producer a moment to update
        if self.interactive_mode() {
            fluvio_future::timer::sleep(Duration::from_millis(100)).await;
        }

        stats_bar.set_message(producer.stats().await.print_current_stats());
        //stats_bar.tick();
        stats_bar.println(line);

        //pb.update_message(format!("{}", producer.stats().await));

        Ok(produce_output)
    }

    async fn produce_key_value(
        &self,
        producer: &TopicProducer,
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

    pub fn metadata() -> FluvioExtensionMetadata {
        FluvioExtensionMetadata {
            title: "produce".into(),
            package: Some("fluvio/fluvio".parse().unwrap()),
            description: "Produce new data in a stream".into(),
            version: semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap(),
        }
    }

    //async fn update_stats(&self, producer: &TopicProducer) -> String {
    //    format!("{}", producer.stats().await)
    //}

    //// How am I going to update this bar w/ current stats?
    //async fn status_bar(&self, producer: Arc<TopicProducer>) -> ProgressRenderer {
    //    if io::stdout().is_tty() {
    //        //pb.enable_steady_tick(100);

    //        if !self.disable_progressbar {
    //            let pb = if !self.no_stats {
    //                let pb = indicatif::ProgressBar::new(100);
    //                pb.set_style(
    //                    indicatif::ProgressStyle::default_bar().template("{spinner} {msg}"),
    //                );
    //                //pb.set_message(format!("{}", producer.stats().await));

    //                pb
    //            } else {
    //                let pb = indicatif::ProgressBar::new(1);
    //                pb.set_style(indicatif::ProgressStyle::default_bar().template("{spinner}"));
    //                pb
    //            };

    //            pb.enable_steady_tick(100);
    //            pb.into()
    //        } else {
    //            indicatif::ProgressBar::hidden().into()
    //        }
    //    } else {
    //        indicatif::ProgressBar::hidden().into()
    //    }
    //}

    /// Initialize Ctrl-C event handler
    async fn init_ctrlc(&self, producer: Arc<TopicProducer>, stats_bar: ProgressBar) -> Result<()> {
        let result = ctrlc::set_handler(move || {
            fluvio_future::task::run_block_on(async {
                // The progress bar will render these in reverse order
                stats_bar.set_message(producer.stats().await.print_summary_stats());
                stats_bar.tick();
                stats_bar.println(producer.stats().await.print_current_stats());
                stats_bar.println(" ");
                //stats_bar.println(producer.stats().await.print_current_stats());
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

    //fn stats_bar(&self, producer: &TopicProducer) {
    //    if io::stdout().is_tty() {
    //        let pb = ProgressRenderer::default();
    //    }
    //}
}
