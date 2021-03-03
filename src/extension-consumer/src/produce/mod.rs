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

    /// Do not print progress output when sending records
    #[structopt(short, long)]
    pub quiet: bool,

    /// Sends key/value records split on the first instance of the separator. Implies --lines.
    #[structopt(long, validator = validate_key_separator)]
    pub key_separator: Option<String>,

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
        let mut lines = BufReader::new(file).lines().enumerate();
        while let Some((i, Ok(line))) = lines.next() {
            if self.kv_mode() {
                self.produce_key_value(producer, &line).await?;
            } else {
                producer.send_record(&line, self.partition).await?;
            }
            if !self.quiet {
                println!("{}:{} {}", path.display(), i, line);
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
        }
        if !self.quiet {
            println!("{}", path.display());
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
        let mut stdin_lines = BufReader::new(std::io::stdin()).lines().enumerate();
        while let Some((i, Ok(line))) = stdin_lines.next() {
            if self.kv_mode() {
                self.produce_key_value(producer, &line).await?;
            } else {
                producer.send_record(&line, self.partition).await?;
            }
            if !self.quiet {
                println!("stdin:{} {}", i, line);
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
        }
        if !self.quiet {
            println!("produced stdin");
        }
        Ok(())
    }

    fn kv_mode(&self) -> bool {
        self.key_separator.is_some()
    }

    async fn produce_key_value<C: AsRef<[u8]>>(
        &self,
        producer: &mut TopicProducer,
        contents: C,
    ) -> Result<()> {
        let contents = contents.as_ref();

        if let Some(separator) = &self.key_separator {
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
            producer.send(key, value).await?;
            return Ok(());
        }

        Err(ConsumerError::Other(
            "Failed to send key-value record".to_string(),
        ))
    }

    pub fn metadata() -> FluvioExtensionMetadata {
        FluvioExtensionMetadata {
            command: "produce".into(),
            description: "Produce new data in a stream".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}
