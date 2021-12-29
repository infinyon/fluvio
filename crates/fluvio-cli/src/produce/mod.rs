use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::error;

use fluvio::{Fluvio, FluvioError, TopicProducer, TopicProducerConfigBuilder, RecordKey};
use fluvio_types::print_cli_ok;
use crate::common::FluvioExtensionMetadata;
use crate::Result;

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
#[derive(Debug, StructOpt)]
pub struct ProduceOpt {
    /// The name of the Topic to produce to
    #[structopt(value_name = "topic")]
    pub topic: String,

    /// Print progress output when sending records
    #[structopt(short, long)]
    pub verbose: bool,

    /// Sends key/value records split on the first instance of the separator.
    #[structopt(long, validator = validate_key_separator)]
    pub key_separator: Option<String>,

    /// Send all input as one record. Use this when producing binary files.
    #[structopt(long)]
    pub raw: bool,

    /// Path to a file to produce to the topic. If absent, producer will read stdin.
    #[structopt(short, long)]
    pub file: Option<PathBuf>,
}

fn validate_key_separator(separator: String) -> std::result::Result<(), String> {
    if separator.is_empty() {
        return Err(
            "must be non-empty. If using '=', type it as '--key-separator \"=\"'".to_string(),
        );
    }
    Ok(())
}

impl ProduceOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let config = if self.interactive_mode() {
            TopicProducerConfigBuilder::default()
                .linger(std::time::Duration::from_millis(10))
                .build()
                .map_err(FluvioError::from)?
        } else {
            Default::default()
        };
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
            producer.send(RecordKey::NULL, buffer).await?;
        } else {
            // Read input line-by-line and send as individual records
            self.produce_lines(&producer).await?;
        }

        producer.flush().await?;
        if self.interactive_mode() {
            print_cli_ok!();
        }

        Ok(())
    }

    async fn produce_lines(&self, producer: &TopicProducer) -> Result<()> {
        match &self.file {
            Some(path) => {
                let reader = BufReader::new(File::open(path)?);
                for line in reader.lines().filter_map(|it| it.ok()) {
                    self.produce_line(producer, &line).await?;
                }
            }
            None => {
                let mut lines = BufReader::new(std::io::stdin()).lines();
                if self.interactive_mode() {
                    eprint!("> ");
                }
                while let Some(Ok(line)) = lines.next() {
                    self.produce_line(producer, &line).await?;
                    if self.interactive_mode() {
                        print_cli_ok!();
                        eprint!("> ");
                    }
                }
            }
        };

        Ok(())
    }

    async fn produce_line(&self, producer: &TopicProducer, line: &str) -> Result<()> {
        if let Some(separator) = &self.key_separator {
            self.produce_key_value(producer, line, separator).await?;
        } else {
            producer.send(RecordKey::NULL, line).await?;
        }

        Ok(())
    }

    async fn produce_key_value(
        &self,
        producer: &TopicProducer,
        line: &str,
        separator: &str,
    ) -> Result<()> {
        let maybe_kv = line.split_once(separator);
        let (key, value) = match maybe_kv {
            Some(kv) => kv,
            None => {
                error!(
                    "Failed to find separator '{}' in record, skipping: '{}'",
                    separator, line
                );
                return Ok(());
            }
        };

        if self.verbose {
            println!("[{}] {}", key, value);
        }

        producer.send(key, value).await?;
        Ok(())
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
