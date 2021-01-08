use std::fs::File;
use std::io::{BufReader, BufRead};
use std::path::{PathBuf, Path};
use structopt::StructOpt;

use fluvio::{Fluvio, TopicProducer};
use crate::common::FluvioExtensionMetadata;
use crate::Result;

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

    /// Whether to print each line of input as an individual record
    #[structopt(short, long)]
    pub lines: bool,

    /// One or more files to read records from
    #[structopt(short, long)]
    pub files: Vec<PathBuf>,
}

impl ProduceLogOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let mut producer = fluvio.topic_producer(&self.topic).await?;

        if self.files.is_empty() {
            self.produce_from_stdin(&mut producer).await?;
        } else {
            self.produce_from_files(&mut producer).await?;
        }

        Ok(())
    }

    /// Sends each line of stdin as a record
    async fn produce_from_stdin(&self, producer: &mut TopicProducer) -> Result<()> {
        let mut stdin_lines = BufReader::new(std::io::stdin()).lines();
        while let Some(Ok(line)) = stdin_lines.next() {
            producer.send_record(&line, self.partition).await?;
        }
        Ok(())
    }

    /// Sends records to a Topic based on the file configuration given
    async fn produce_from_files(&self, producer: &mut TopicProducer) -> Result<()> {
        for path in &self.files {
            self.produce_from_file(producer, path).await?;
        }

        Ok(())
    }

    /// Produces one or more records from a single file
    async fn produce_from_file(&self, producer: &mut TopicProducer, path: &Path) -> Result<()> {
        if self.lines {
            let file = File::open(path)?;
            let mut lines = BufReader::new(file).lines();
            while let Some(Ok(line)) = lines.next() {
                producer.send_record(&line, self.partition).await?;
            }
        } else {
            let bytes = std::fs::read(path)?;
            producer.send_record(&bytes, self.partition).await?;
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
