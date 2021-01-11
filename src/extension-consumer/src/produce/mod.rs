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

    /// Send messages in an infinite loop
    #[structopt(short = "C", long = "continuous")]
    pub continuous: bool,

    /// Send each line of the file as its own Record
    #[structopt(
        short = "l",
        long = "record-per-line",
        value_name = "filename",
        parse(from_os_str)
    )]
    record_per_line: Option<PathBuf>,

    /// Send an entire file as a single Record
    #[structopt(
        short = "r",
        long = "record-file",
        value_name = "filename",
        parse(from_os_str),
        conflicts_with = "record-per-line"
    )]
    record_file: Vec<PathBuf>,
}

impl ProduceLogOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let (cfg, file_records) = self.validate()?;
        let mut producer = fluvio.topic_producer(&cfg.topic).await?;

        if let Some(records) = file_records {
            produce_from_files(&mut producer, cfg, records).await?;
        } else {
            produce_stdin(&mut producer, cfg).await?;
        }

        Ok(())
    }

    /// Validate cli options. Generate target-server and produce log configuration.
    pub fn validate(self) -> Result<(ProduceLogConfig, Option<FileRecord>)> {
        let file_records = if let Some(record_per_line) = self.record_per_line {
            Some(FileRecord::Lines(record_per_line))
        } else if !self.record_file.is_empty() {
            Some(FileRecord::Files(self.record_file))
        } else {
            None
        };

        let produce_log_cfg = ProduceLogConfig {
            topic: self.topic,
            partition: self.partition,
            continuous: self.continuous,
        };

        Ok((produce_log_cfg, file_records))
    }

    pub fn metadata() -> FluvioExtensionMetadata {
        FluvioExtensionMetadata {
            command: "produce".into(),
            description: "Produce new data in a stream".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

/// Sends records to a Topic based on the file configuration given
///
/// This will either send the lines of a single file as individual records,
/// or it will send the entirety of a list of files as records, where each
/// whole file is one record.
async fn produce_from_files(
    producer: &mut TopicProducer,
    cfg: ProduceLogConfig,
    records: FileRecord,
) -> Result<()> {
    match records {
        FileRecord::Files(paths) => {
            for path in paths {
                let bytes = std::fs::read(&path)?;
                producer.send_record(&bytes, cfg.partition).await?;
                print_cli_ok!();
            }
        }
        FileRecord::Lines(path) => {
            let file = File::open(&path)?;
            let mut lines = BufReader::new(file).lines();
            while let Some(Ok(line)) = lines.next() {
                producer.send_record(&line, cfg.partition).await?;
            }
            print_cli_ok!();
        }
    }

    Ok(())
}

/// Sends each line of stdin as a record
async fn produce_stdin(producer: &mut TopicProducer, cfg: ProduceLogConfig) -> Result<()> {
    let mut stdin_lines = BufReader::new(std::io::stdin()).lines();
    while let Some(Ok(line)) = stdin_lines.next() {
        producer.send_record(&line, cfg.partition).await?;
        print_cli_ok!();
    }
    Ok(())
}
