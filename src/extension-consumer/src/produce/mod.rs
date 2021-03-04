use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::{PathBuf, Path};
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
/// By default, this reads input from stdin until EOF, and sends the
/// contents as one record. Alternatively, input files may be given with
/// '--files' and input may be sent line-by-line with '--lines'.
///
/// If '--key-separator' or '--json-path' are used, records are sent as
/// key/value pairs. In this case, '--partition' is ignored and the partition
/// each record is sent to is derived from the record's key.
#[derive(Debug, StructOpt)]
pub struct ProduceLogOpt {
    /// The name of the Topic to produce to
    #[structopt(value_name = "topic")]
    pub topic: String,

    /// The ID of the Partition to produce to
    #[structopt(
        short = "p",
        long = "partition",
        value_name = "integer",
        default_value = "0"
    )]
    pub partition: i32,

    /// Send each line of input as its own record (using '\n')
    #[structopt(short, long)]
    pub lines: bool,

    /// Print progress output when sending records
    #[structopt(short, long)]
    pub verbose: bool,

    /// Sends key/value records split on the first instance of the separator. Implies --lines.
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
            if atty::is(atty::Stream::Stdin) {
                if self.lines {
                    eprintln!("Reading one record per line from stdin:");
                } else {
                    eprintln!("Reading one record from stdin (use ctrl-D to send):");
                }
            }
            self.produce_stdin(&mut producer).await?;
        } else {
            self.produce_from_files(&mut producer).await?;
        }

        Ok(())
    }

    /// Sends records to a Topic based on the file configuration given
    ///
    /// This will either send the lines of a single file as individual records,
    /// or it will send the entirety of a list of files as records, where each
    /// whole file is one record.
    async fn produce_from_files(&self, producer: &mut TopicProducer) -> Result<()> {
        for path in &self.files {
            if self.lines {
                self.produce_file_lines(producer, path).await?;
            } else {
                self.produce_file_whole(producer, path).await?;
            }
            print_cli_ok!();
        }

        Ok(())
    }

    /// Sends each line from a file as a unique record to the producer's topic.
    async fn produce_file_lines(&self, producer: &mut TopicProducer, path: &Path) -> Result<()> {
        let file = File::open(path)?;
        let mut lines = BufReader::new(file).lines();
        while let Some(Ok(line)) = lines.next() {
            if self.kv_mode() {
                self.produce_key_value(producer, line.as_bytes()).await?;
            } else {
                producer.send_record(&line, self.partition).await?;
                if self.verbose {
                    println!("[null] {}", line);
                }
            }
        }
        Ok(())
    }

    /// Sends an entire file as one record to the producer's topic.
    async fn produce_file_whole(&self, producer: &mut TopicProducer, path: &Path) -> Result<()> {
        let bytes = std::fs::read(path)?;
        if self.kv_mode() {
            self.produce_key_value(producer, &bytes).await?;
        } else {
            producer.send_record(&bytes, self.partition).await?;
            if self.verbose {
                println!("[null]");
            }
        }
        Ok(())
    }

    /// Sends stdin as one ore more records
    async fn produce_stdin(&self, producer: &mut TopicProducer) -> Result<()> {
        if self.lines {
            self.produce_stdin_lines(producer).await?;
        } else {
            self.produce_stdin_whole(producer).await?;
        }
        print_cli_ok!();
        Ok(())
    }

    /// Sends each line of stdin as a unique record
    async fn produce_stdin_lines(&self, producer: &mut TopicProducer) -> Result<()> {
        let mut stdin_lines = BufReader::new(std::io::stdin()).lines();
        while let Some(Ok(line)) = stdin_lines.next() {
            if self.kv_mode() {
                self.produce_key_value(producer, line.as_bytes()).await?;
            } else {
                producer.send_record(&line, self.partition).await?;
                if self.verbose {
                    println!("[null] {}", line);
                }
            }
            if atty::is(atty::Stream::Stdin) {
                print_cli_ok!();
            }
        }
        Ok(())
    }

    /// Sends the entire contents of stdin as one record
    async fn produce_stdin_whole(&self, producer: &mut TopicProducer) -> Result<()> {
        use std::io::Read;
        let mut buffer = vec![];
        std::io::stdin().read_to_end(&mut buffer)?;
        if self.kv_mode() {
            self.produce_key_value(producer, &buffer).await?;
        } else {
            producer.send_record(&buffer, self.partition).await?;
            if self.verbose {
                println!("[null]");
            }
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
