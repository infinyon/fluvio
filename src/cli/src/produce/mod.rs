use std::path::PathBuf;
use std::sync::Arc;

use tracing::debug;
use structopt::StructOpt;

use fluvio::Fluvio;

use crate::Result;
use crate::Terminal;

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
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let (cfg, file_records) = self.validate()?;
        let producer = fluvio.topic_producer(&cfg.topic).await?;

        debug!("got producer");
        if let Some(records) = file_records {
            produce::produce_file_records(producer, out, cfg, records).await?;
        } else {
            produce::produce_from_stdin(producer, cfg).await?;
        }

        Ok(())
    }

    /// Validate cli options. Generate target-server and produce log configuration.
    pub fn validate(self) -> Result<(ProduceLogConfig, Option<FileRecord>)> {
        let file_records = if let Some(record_per_line) = self.record_per_line {
            Some(FileRecord::Lines(record_per_line))
        } else if !self.record_file.is_empty() {
            Some(FileRecord::Files(self.record_file.clone()))
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
}

#[allow(clippy::module_inception)]
mod produce {
    use tracing::debug;
    use futures_lite::StreamExt;

    use fluvio_future::fs::File;
    use fluvio_future::io::stdin;
    use fluvio_future::io::ReadExt;
    use futures_lite::io::BufReader;
    use futures_lite::io::AsyncBufReadExt;
    use fluvio_types::{print_cli_err, print_cli_ok};
    use fluvio::TopicProducer;

    use crate::t_println;

    use super::*;

    pub type RecordTuples = Vec<(String, Vec<u8>)>;

    pub async fn produce_file_records<O: Terminal>(
        mut producer: TopicProducer,
        out: std::sync::Arc<O>,
        cfg: ProduceLogConfig,
        file: FileRecord,
    ) -> Result<()> {
        let tuples = file_to_records(file).await?;
        for r_tuple in tuples {
            t_println!(out, "{}", r_tuple.0);
            process_record(&mut producer, cfg.partition, r_tuple.1).await;
        }
        Ok(())
    }

    /// Dispatch records based on the content of the record tuples variable
    pub async fn produce_from_stdin(
        mut producer: TopicProducer,
        opt: ProduceLogConfig,
    ) -> Result<()> {
        let stdin = stdin();
        let mut lines = BufReader::new(stdin).lines();
        while let Some(line) = lines.next().await {
            let text = line?;
            debug!("read lines {} bytes", text);
            let record = text.as_bytes().to_vec();
            process_record(&mut producer, opt.partition, record).await;
            if !opt.continuous {
                return Ok(());
            }
        }

        debug!("done sending records");

        Ok(())
    }

    /// Process record and print success or error
    /// TODO: Add version handling for SPU
    async fn process_record(producer: &mut TopicProducer, partition: i32, record: Vec<u8>) {
        match producer.send_record(record, partition).await {
            Ok(()) => {
                debug!("record send success");
                print_cli_ok!()
            }
            Err(err) => {
                print_cli_err!(format!("error processing record: {}", err));
                std::process::exit(-1);
            }
        }
    }

    /// Retrieve one or more files and converts them into a list of (name, record) touples
    async fn file_to_records(file_record_options: FileRecord) -> Result<RecordTuples> {
        let mut records: RecordTuples = vec![];

        match file_record_options {
            // lines as records
            FileRecord::Lines(lines2rec_path) => {
                let f = File::open(lines2rec_path).await?;
                let mut lines = BufReader::new(f).lines();
                // reach each line and convert to byte array
                if let Some(line) = lines.next().await {
                    if let Ok(text) = line {
                        records.push((text.clone(), text.as_bytes().to_vec()));
                    }
                }
            }

            // files as records
            FileRecord::Files(files_to_rec_path) => {
                for file_path in files_to_rec_path {
                    let file_name = file_path.to_str().unwrap_or("?");
                    let mut f = File::open(&file_path).await?;
                    let mut buffer = Vec::new();

                    // read the whole file in a byte array
                    f.read_to_end(&mut buffer).await?;
                    records.push((file_name.to_owned(), buffer));
                }
            }
        }

        Ok(records)
    }
}
