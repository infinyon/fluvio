//!
//! # Produce CLI
//!
//! CLI command for Produce operation
//!

use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufRead;
use std::fs::File;
use std::path::PathBuf;

use structopt::StructOpt;

use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::helpers::process_sc_produce_record;
use super::helpers::process_spu_produce_record;
use super::helpers::process_kf_produce_record;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct ProduceLogOpt {
    /// Topic name
    #[structopt(short = "t", long = "topic", value_name = "string")]
    pub topic: String,

    /// Partition id
    #[structopt(short = "p", long = "partition", value_name = "integer")]
    pub partition: i32,

    /// Send messages in an infinite loop
    #[structopt(short = "C", long = "continuous")]
    pub continuous: bool,

    /// Each line is a Record
    #[structopt(
        short = "l",
        long = "record-per-line",
        value_name = "filename",
        parse(from_os_str)
    )]
    record_per_line: Option<PathBuf>,

    /// Entire file is a Record (multiple)
    #[structopt(
        short = "r",
        long = "record-file",
        value_name = "filename",
        parse(from_os_str),
        conflicts_with = "record_per_line"
    )]
    record_file: Vec<PathBuf>,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    pub sc: Option<String>,

    ///Address of Streaming Processing Unit
    #[structopt(
        short = "u",
        long = "spu",
        value_name = "host:port",
        conflicts_with = "sc"
    )]
    pub spu: Option<String>,

    /// Address of Kafka Controller
    #[structopt(
        short = "k",
        long = "kf",
        value_name = "host:port",
        conflicts_with = "sc",
        conflicts_with = "spu"
    )]
    pub kf: Option<String>,

    ///Profile name
    #[structopt(short = "P", long = "profile")]
    pub profile: Option<String>,
}

// -----------------------------------
//  Parsed Config
// -----------------------------------

/// Produce log configuration parameters
#[derive(Debug)]
pub struct ProduceLogConfig {
    pub topic: String,
    pub partition: i32,

    pub records_form_file: Option<FileRecord>,
}

#[derive(Debug)]
pub enum FileRecord {
    Lines(PathBuf),
    Files(Vec<PathBuf>),
}

pub type RecordTouples = Vec<(String, Vec<u8>)>;

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process produce record cli request
pub fn process_produce_record(opt: ProduceLogOpt) -> Result<(), CliError> {
    let (target_server, produce_log_cfg, continous) = parse_opt(opt)?;
    let file_records = file_to_records(&produce_log_cfg.records_form_file)?;
    let topic = produce_log_cfg.topic.clone();
    let partition = produce_log_cfg.partition;

    match target_server {
        TargetServer::Kf(server_addr) => {
            process_kf_produce_record(server_addr, topic, partition, file_records,continous)
        }
        TargetServer::Spu(server_addr) => {
            process_spu_produce_record(server_addr, topic, partition, file_records,continous)
        }
        TargetServer::Sc(server_addr) => {
            process_sc_produce_record(server_addr, topic, partition, file_records, continous)
        }
    }
}

/// Validate cli options. Generate target-server and produce log configuration.
fn parse_opt(opt: ProduceLogOpt) -> Result<(TargetServer, ProduceLogConfig,bool), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new_with_spu(&opt.sc, &opt.spu, &opt.kf, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // generate file record
    let records_from_file = if let Some(record_per_line) = opt.record_per_line {
        Some(FileRecord::Lines(record_per_line.clone()))
    } else if opt.record_file.len() > 0 {
        Some(FileRecord::Files(opt.record_file.clone()))
    } else {
        None
    };

    // produce log specific configurations
    let produce_log_cfg = ProduceLogConfig {
        topic: opt.topic,
        partition: opt.partition,
        records_form_file: records_from_file,
    };

    // return server separately from config
    Ok((target_server, produce_log_cfg,opt.continuous))
}

/// Retrieve one or more files and converts them into a list of (name, record) touples
pub fn file_to_records(
    file_record_options: &Option<FileRecord>,
) -> Result<RecordTouples, CliError> {
    let mut records: RecordTouples = vec![];

    match file_record_options {
        Some(file_record) => {
            match file_record {
                // lines as records
                FileRecord::Lines(lines2rec_path) => {
                    let f = File::open(lines2rec_path)?;
                    let file = BufReader::new(&f);

                    // reach each line and conver to byte array
                    for line in file.lines() {
                        if let Ok(text) = line {
                            records.push((text.clone(), text.as_bytes().to_vec()));
                        }
                    }
                }

                // files as records
                FileRecord::Files(files_to_rec_path) => {
                    for file_path in files_to_rec_path {
                        let file_name = file_path.to_str().unwrap_or("?");
                        let mut f = File::open(file_path)?;
                        let mut buffer = Vec::new();

                        // read the whole file in a byte array
                        f.read_to_end(&mut buffer)?;
                        records.push((file_name.to_owned(), buffer));
                    }
                }
            }
        }
        None => {}
    }

    Ok(records)
}
