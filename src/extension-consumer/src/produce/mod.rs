use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use structopt::StructOpt;
use tracing::debug;

use fluvio::{Fluvio, TopicProducer};
use fluvio_types::print_cli_ok;
use crate::common::FluvioExtensionMetadata;
use crate::{Result, ConsumerError};

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
pub struct ProduceLogOpt {
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

impl ProduceLogOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let mut producer = fluvio.topic_producer(&self.topic).await?;

        match &self.file {
            Some(path) => {
                let mut reader = BufReader::new(File::open(path)?);
                self.produce_lines(&mut producer, &mut reader).await?;
            }
            None => {
                let mut reader = BufReader::new(std::io::stdin());
                self.produce_lines(&mut producer, &mut reader).await?;
            }
        };

        if !self.interactive_mode() {
            print_cli_ok!();
        }

        Ok(())
    }

    async fn produce_lines<B>(&self, producer: &mut TopicProducer, input: &mut B) -> Result<()>
    where
        B: BufRead,
    {
        let mut lines = input.lines();
        if self.interactive_mode() {
            eprint!("> ");
        }
        while let Some(Ok(line)) = lines.next() {
            self.produce_str(producer, &line).await?;
            if self.interactive_mode() {
                print_cli_ok!();
                eprint!("> ");
            }
        }
        Ok(())
    }

    async fn produce_str(&self, producer: &mut TopicProducer, string: &str) -> Result<()> {
        if self.kv_mode() {
            self.produce_key_value(producer, string).await?;
        } else {
            producer.send_record(string, 0).await?;
            if self.verbose {
                println!("[null] {}", string);
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

    async fn produce_key_value(&self, producer: &mut TopicProducer, string: &str) -> Result<()> {
        if let Some(separator) = &self.key_separator {
            self.produce_key_value_via_separator(producer, string, separator)
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
        string: &str,
        separator: &str,
    ) -> Result<()> {
        debug!(?separator, "Producing Key/Value:");

        let pieces: Vec<_> = string.split(separator).collect();
        if pieces.len() < 2 {
            return Err(ConsumerError::Other(format!(
                "Failed to find separator '{}' in record '{}'",
                separator, string
            )));
        }

        let key = pieces[0];
        let value: String = (&pieces[1..]).join(&*separator);
        producer.send(key, &value).await?;
        if self.verbose {
            println!("[{}] {}", key, value);
        }
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
