use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use futures::future::join_all;
use clap::Parser;
use tracing::error;
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

    /// Print out client stats
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

        let config = config_builder.build().map_err(FluvioError::from)?;
        let producer = fluvio
            .topic_producer_with_config(&self.topic, config)
            .await?;

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
            produce_output.wait().await?;
        } else {
            // Read input line-by-line and send as individual records
            self.produce_lines(&producer).await?;
        }

        producer.flush().await?;
        if self.interactive_mode() {
            print_cli_ok!();
            // I should print the client stats here
        }

        Ok(())
    }

    async fn produce_lines(&self, producer: &TopicProducer) -> Result<()> {
        match &self.file {
            Some(path) => {
                let reader = BufReader::new(File::open(path)?);
                let mut produce_outputs = vec![];
                for line in reader.lines().filter_map(|it| it.ok()) {
                    let produce_output = self.produce_line(producer, &line).await?;
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
                    let produce_output = self.produce_line(producer, &line).await?;
                    if let Some(produce_output) = produce_output {
                        // ensure it was properly sent
                        produce_output.wait().await?;
                    }
                    if self.interactive_mode() {
                        print_cli_ok!();
                        if self.stats {
                            println!("{:?}", producer.stats().await);
                        }
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
    ) -> Result<Option<ProduceOutput>> {
        let produce_output = if let Some(separator) = &self.key_separator {
            self.produce_key_value(producer, line, separator).await?
        } else {
            Some(producer.send(RecordKey::NULL, line).await?)
        };
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
