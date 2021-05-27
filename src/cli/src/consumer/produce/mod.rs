use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::{error, debug};

use fluvio::{Fluvio, TopicProducer, RecordKey};
use fluvio_types::print_cli_ok;
use crate::common::FluvioExtensionMetadata;
use crate::consumer::error::ConsumerError;

const DEFAULT_BATCH_SIZE: usize = 50;

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
    pub async fn process(self, fluvio: &Fluvio) -> Result<(), ConsumerError> {
        let mut producer = fluvio.topic_producer(&self.topic).await?;

        match &self.file {
            Some(path) => {
                let reader = BufReader::new(File::open(path)?);
                let lines: Vec<_> = reader.lines().filter_map(|it| it.ok()).collect();
                let batches: Vec<_> = lines.chunks(DEFAULT_BATCH_SIZE).collect();
                for batch in batches {
                    let batch: Vec<_> = batch.iter().map(|it| &**it).collect();
                    self.produce_lines(&mut producer, &batch).await?;
                }
            }
            None => {
                let mut lines = BufReader::new(std::io::stdin()).lines();
                if self.interactive_mode() {
                    eprint!("> ");
                }
                while let Some(Ok(line)) = lines.next() {
                    self.produce_lines(&mut producer, &[&line]).await?;
                }
            }
        };

        if !self.interactive_mode() {
            print_cli_ok!();
        }

        Ok(())
    }

    async fn produce_lines(
        &self,
        producer: &mut TopicProducer,
        lines: &[&str],
    ) -> Result<(), ConsumerError> {
        self.produce_strs(producer, lines).await?;
        if self.interactive_mode() {
            print_cli_ok!();
            eprint!("> ");
        }
        Ok(())
    }

    async fn produce_strs(
        &self,
        producer: &mut TopicProducer,
        strings: &[&str],
    ) -> Result<(), ConsumerError> {
        if self.kv_mode() {
            self.produce_key_values(producer, strings).await?;
        } else {
            let batch = strings.iter().map(|&s| (RecordKey::NULL, s));
            producer.send_all(batch).await?;
            if self.verbose {
                for string in strings {
                    println!("[null] {}", string);
                }
            }
        }
        Ok(())
    }

    fn kv_mode(&self) -> bool {
        self.key_separator.is_some()
    }

    fn interactive_mode(&self) -> bool {
        self.file.is_none() && atty::is(atty::Stream::Stdin)
    }

    async fn produce_key_values(
        &self,
        producer: &mut TopicProducer,
        strings: &[&str],
    ) -> Result<(), ConsumerError> {
        if let Some(separator) = &self.key_separator {
            self.produce_key_value_via_separator(producer, strings, separator)
                .await?;
            return Ok(());
        }

        Err(ConsumerError::Other(
            "Failed to send key-value record".to_string(),
        ))
    }

    async fn produce_key_value_via_separator(
        &self,
        producer: &mut TopicProducer,
        strings: &[&str],
        separator: &str,
    ) -> Result<(), ConsumerError> {
        debug!(?separator, "Producing Key/Value:");

        let pairs: Vec<_> = strings
            .iter()
            .filter_map(|s| {
                let pieces: Vec<_> = s.split(separator).collect();
                if pieces.len() < 2 {
                    error!("Failed to find separator '{}' in record '{}'", separator, s);
                    return None;
                }

                let key: &str = pieces[0];
                let value: String = (&pieces[1..]).join(&*separator);
                if self.verbose {
                    println!("[{}] {}", key, value);
                }
                Some((key, value))
            })
            .collect();

        producer.send_all(pairs).await?;
        Ok(())
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
