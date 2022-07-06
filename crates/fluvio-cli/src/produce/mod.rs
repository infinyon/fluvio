use std::sync::Arc;
use std::fs::File;
use std::io::{BufReader, BufRead};
use std::{io::Error as IoError, path::PathBuf};
use std::io::{ErrorKind, self};
use crossterm::tty::IsTty;
use futures::future::join_all;
use clap::Parser;
use indicatif::ProgressBar;
use tracing::{debug, error};
use std::time::Duration;
use humantime::parse_duration;

use fluvio::{
    Compression, Fluvio, FluvioError, TopicProducer, TopicProducerConfigBuilder, RecordKey,
    ProduceOutput, ClientStats,
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

    /// Display producer session statistics
    #[clap(long)]
    pub stats: bool,
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

        // Stats
        let config_builder = if self.stats {
            config_builder.stats(self.stats)
        } else {
            config_builder
        };

        let config = config_builder.build().map_err(FluvioError::from)?;
        let producer = Arc::new(
            fluvio
                .topic_producer_with_config(&self.topic, config)
                .await?,
        );

        let maybe_stats_bar = if io::stdout().is_tty() {
            if self.stats {
                let stats_bar = indicatif::ProgressBar::with_draw_target(
                    100,
                    indicatif::ProgressDrawTarget::stderr(),
                );
                stats_bar.set_style(indicatif::ProgressStyle::default_bar().template("{msg}"));

                // Handle ctrl+c to print summary stats
                init_ctrlc(producer.clone(), stats_bar.clone()).await?;

                Some(stats_bar)
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

            if self.stats {
                if let (Some(stats_bar), Some(producer_stats)) =
                    (&maybe_stats_bar, producer.stats().await)
                {
                    stats_bar.set_message(format_current_stats(producer_stats.snapshot()).await);
                }
            }

            produce_output.wait().await?;
        } else {
            // Read input line-by-line and send as individual records
            self.produce_lines(&producer, &maybe_stats_bar).await?;
        };

        if self.stats {
            if let Some(stats_bar) = &maybe_stats_bar {
                producer_summary(producer.clone(), stats_bar).await;
            }
        }

        producer.flush().await?;
        if self.interactive_mode() {
            print_cli_ok!();
        }

        Ok(())
    }

    async fn produce_lines(
        &self,
        producer: &TopicProducer,
        maybe_stats_bar: &Option<ProgressBar>,
    ) -> Result<()> {
        match &self.file {
            Some(path) => {
                let reader = BufReader::new(File::open(path)?);
                let mut produce_outputs = vec![];
                for line in reader.lines().filter_map(|it| it.ok()) {
                    let produce_output =
                        self.produce_line(producer, &line, maybe_stats_bar).await?;

                    if let Some(produce_output) = produce_output {
                        produce_outputs.push(produce_output);
                    }
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
            }
            None => {
                let mut lines = BufReader::new(std::io::stdin()).lines();
                if self.interactive_mode() {
                    eprint!("> ");
                }
                while let Some(Ok(line)) = lines.next() {
                    let produce_output =
                        self.produce_line(producer, &line, maybe_stats_bar).await?;
                    if let Some(produce_output) = produce_output {
                        // ensure it was properly sent
                        produce_output.wait().await?;
                    }

                    if self.interactive_mode() {
                        print_cli_ok!();
                        eprint!("> ");
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
        maybe_stats_bar: &Option<ProgressBar>,
    ) -> Result<Option<ProduceOutput>> {
        let produce_output = if let Some(separator) = &self.key_separator {
            self.produce_key_value(producer, line, separator).await?
        } else {
            Some(producer.send(RecordKey::NULL, line).await?)
        };

        if self.stats {
            if let (Some(stats_bar), Some(producer_stats)) =
                (maybe_stats_bar, producer.stats().await)
            {
                stats_bar.set_message(format_current_stats(producer_stats.snapshot()).await);

                if self.interactive_mode() {
                    stats_bar.println(line);
                }
            }
        }

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
}

/// Return String with details the current stats
async fn format_current_stats(client_stats: ClientStats) -> String {
    format!(
        "throughput: {:<15.3} latency: {:<15.3} memory: {:<10.3} CPU: {:<5.2}",
        client_stats.throughput(),
        client_stats.last_latency(),
        client_stats.memory(),
        client_stats.cpu()
    )
}

/// Return String with a formatted summary of the current stats
async fn format_summary_stats(client_stats: ClientStats) -> String {
    format!(
            "total throughput: {:<15.3} total data: {:<10.3} records transferred: {:<10} uptime: {:<10.3}",
            client_stats.total_throughput(),
            client_stats.total_bytes(),
            client_stats.records(),
            client_stats.uptime(),
        )
}

/// Initialize Ctrl-C event handler
async fn init_ctrlc(producer: Arc<TopicProducer>, stats_bar: ProgressBar) -> Result<()> {
    let result = ctrlc::set_handler(move || {
        fluvio_future::task::run_block_on(async {
            producer_summary(producer.clone(), &stats_bar).await;
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

async fn producer_summary(producer: Arc<TopicProducer>, stats_bar: &ProgressBar) {
    if let Some(producer_stats) = producer.stats().await {
        // The progress bar will render these in reverse order
        stats_bar.set_message(format_summary_stats(producer_stats.snapshot()).await);
        stats_bar.println(format_current_stats(producer_stats.snapshot()).await);
        stats_bar.println(" ");
    }
}
