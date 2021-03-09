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
/// When no '--files' are provided, the producer will read from 'stdin'
/// and send each line of input as one record.
///
/// If one or more files are given with '--files', each file is sent as
/// one entire record.
///
/// If '--key-separator' or '--json-path' are used, records are sent as
/// key/value pairs, and the keys are used to determine which partition
/// the records are sent to.
#[derive(Debug, StructOpt)]
pub struct ProduceLogOpt {
    /// The name of the Topic to produce to
    #[structopt(value_name = "topic")]
    pub topic: String,

    /// Send each line of input as its own record (using '\n')
    #[structopt(hidden = true, short, long)]
    pub lines: bool,

    /// Print progress output when sending records
    #[structopt(short, long)]
    pub verbose: bool,

    /// Sends key/value records split on the first instance of the separator.
    #[structopt(long, validator = validate_key_separator)]
    pub key_separator: Option<String>,

    /// Sends key/value JSON records where the key is selected using this JSON path.
    #[structopt(long, conflicts_with("key-separator"))]
    pub json_path: Option<String>,

    /// Paths to files to produce to the topic. If absent, producer will read stdin.
    #[structopt(short, long)]
    pub files: Vec<PathBuf>,
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
    pub async fn process(mut self, fluvio: &Fluvio) -> Result<()> {
        let mut producer = fluvio.topic_producer(&self.topic).await?;

        // --key-separator implies --lines
        if self.key_separator.is_some() {
            self.lines = true;
        }

        if self.files.is_empty() {
            self.produce_stdin(&mut producer).await?;
        } else {
            self.produce_files(&mut producer).await?;
        }

        Ok(())
    }

    async fn produce_stdin(&self, producer: &mut TopicProducer) -> Result<()> {
        if atty::is(atty::Stream::Stdin) {
            eprintln!("Reading one record per line from stdin:");
        }
        let mut reader = BufReader::new(std::io::stdin());
        self.produce_lines(producer, &mut reader).await?;
        Ok(())
    }

    async fn produce_files(&self, producer: &mut TopicProducer) -> Result<()> {
        for path in &self.files {
            if self.lines {
                let mut reader = BufReader::new(File::open(path)?);
                self.produce_lines(producer, &mut reader).await?;
            } else {
                let buffer = std::fs::read(path)?;
                self.produce_buffer(producer, &buffer).await?;
                if self.verbose {
                    println!("[null]");
                }
            }
        }
        print_cli_ok!();
        Ok(())
    }

    async fn produce_lines<B>(&self, producer: &mut TopicProducer, input: &mut B) -> Result<()>
    where
        B: BufRead,
    {
        let mut lines = input.lines();
        while let Some(Ok(line)) = lines.next() {
            self.produce_buffer(producer, line.as_bytes()).await?;
            if self.verbose {
                println!("[null] {}", line);
            }
            print_cli_ok!();
        }
        Ok(())
    }

    async fn produce_buffer(&self, producer: &mut TopicProducer, buffer: &[u8]) -> Result<()> {
        if self.kv_mode() {
            self.produce_key_value(producer, buffer).await?;
        } else {
            producer.send_record(buffer, 0).await?;
        }
        Ok(())
    }

    fn kv_mode(&self) -> bool {
        self.key_separator.is_some() || self.json_path.is_some()
    }

    async fn produce_key_value(&self, producer: &mut TopicProducer, contents: &[u8]) -> Result<()> {
        if let Some(separator) = &self.key_separator {
            self.produce_key_value_via_separator(producer, contents, separator)
                .await?;
            return Ok(());
        }

        if let Some(jsonpath) = &self.json_path {
            self.produce_key_value_via_jsonpath(producer, contents, jsonpath)
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
        contents: &[u8],
        separator: &str,
    ) -> Result<()> {
        debug!(?separator, "Producing Key/Value:");
        let string = std::str::from_utf8(contents).map_err(|_| {
            ConsumerError::Other("--key-separator requires records to be UTF-8".to_string())
        })?;

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

    async fn produce_key_value_via_jsonpath(
        &self,
        producer: &mut TopicProducer,
        contents: &[u8],
        jsonpath: &str,
    ) -> Result<()> {
        let string =
            std::str::from_utf8(contents).map_err(|e| ConsumerError::Other(e.to_string()))?;
        let json: serde_json::Value =
            serde_json::from_str(string).map_err(|e| ConsumerError::Other(e.to_string()))?;
        let selector =
            jsonpath::Selector::new(jsonpath).map_err(|e| ConsumerError::Other(e.to_string()))?;
        let key_results: Vec<_> = selector.find(&json).collect();
        let len = key_results.len();
        if len != 1 {
            return Err(ConsumerError::Other(format!(
                "Jsonpath must select 1 key: found {}",
                len
            )));
        }

        let key = key_results[0];
        let key_string = key
            .as_str()
            .ok_or_else(|| ConsumerError::Other("Selected value must be a string".to_string()))?;
        producer.send(key_string, contents).await?;
        if self.verbose {
            println!("[{}] {}", key_string, string);
        }
        Ok(())
    }

    pub fn metadata() -> FluvioExtensionMetadata {
        FluvioExtensionMetadata {
            command: "produce".into(),
            description: "Produce new data in a stream".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}
