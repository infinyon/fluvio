use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::PathBuf;
use structopt::StructOpt;

use fluvio::{Fluvio, TopicProducer};
use fluvio_types::print_cli_ok;
use crate::common::FluvioExtensionMetadata;
use crate::Result;

/// Produce log configuration parameters
#[derive(Debug)]
pub struct ProduceLogConfig {
    pub topic: String,
    pub partition: i32,
    pub continuous: bool,
}

#[derive(Debug)]
pub enum FileRecord {
    Lines(PathBuf),
    Files(Vec<PathBuf>),
}

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

    /// Paths to files to produce to the topic. If absent, producer will read stdin.
    #[structopt(short, long)]
    pub files: Vec<PathBuf>,
}

impl ProduceLogOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let mut producer = fluvio.topic_producer(&self.topic).await?;

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
                let file = File::open(path)?;
                let mut lines = BufReader::new(file).lines().enumerate();
                while let Some((i, Ok(line))) = lines.next() {
                    producer.send_record(&line, self.partition).await?;
                    if !self.quiet {
                        println!("{}:{} {}", path.display(), i, line);
                    }
                }
            } else {
                let bytes = std::fs::read(path)?;
                producer.send_record(&bytes, self.partition).await?;
                if !self.quiet {
                    println!("{}", path.display());
                }
            }
            print_cli_ok!();
        }

        Ok(())
    }

    /// Sends each line of stdin as a record
    async fn produce_stdin(&self, producer: &mut TopicProducer) -> Result<()> {
        use std::io::Read;
        if self.lines {
            let mut stdin_lines = BufReader::new(std::io::stdin()).lines().enumerate();
            while let Some((i, Ok(line))) = stdin_lines.next() {
                producer.send_record(&line, self.partition).await?;
                if !self.quiet {
                    println!("stdin:{} {}", i, line);
                }
            }
        } else {
            let mut buffer = vec![];
            std::io::stdin().read_to_end(&mut buffer)?;
            producer.send_record(&buffer, self.partition).await?;
            if !self.quiet {
                println!("produced stdin");
            }
        }
        print_cli_ok!();
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
