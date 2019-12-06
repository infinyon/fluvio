//!
//! # Fluvio Fetch Logs
//!
//! Connects to server and fetches logs
//!

use std::io::{self, Write};

use serde_json;
use serde_json::Value;
use types::print_cli_err;

use kf_protocol::api::DefaultRecords;
use kf_protocol::message::fetch::DefaultKfFetchResponse;
use kf_protocol::message::fetch::FetchablePartitionResponse;

use crate::error::CliError;
use crate::common::ConsumeOutputType;
use crate::common::{bytes_to_hex_dump, hex_dump_separator};

/// Consume log configuration parameters
#[derive(Debug, Clone)]
pub struct ReponseLogParams {
    pub output: ConsumeOutputType,
    pub suppress: bool,
}

/// Process fetch topic response based on output type
pub fn process_fetch_topic_reponse(
    response: &DefaultKfFetchResponse,
    params: &ReponseLogParams,
) -> Result<(), CliError> {
    // validate topic
    for topic_res in &response.topics {
        // ensure response topic has partitions
        if topic_res.partitions.len() == 0 {
            let err = format!("topic '{}' has no partitions", topic_res.name);
            print_cli_err!(err);
            continue;
        }

        // parse records based on output type
        let partitions_res = &topic_res.partitions;
        match params.output {
            ConsumeOutputType::json => {
                let records =
                    generate_json_records(&topic_res.name, partitions_res, params.suppress);
                print_json_records(&records);
            }
            ConsumeOutputType::text => {
                print_text_records(&topic_res.name, partitions_res, params.suppress);
            }
            ConsumeOutputType::binary => {
                print_binary_records(&topic_res.name, partitions_res);
            }
            ConsumeOutputType::dynamic => {
                print_dynamic_records(&topic_res.name, partitions_res);
            }
            ConsumeOutputType::raw => {
                print_raw_records(&topic_res.name, partitions_res);
            }
        }
    }

    Ok(())
}

// -----------------------------------
//  JSON
// -----------------------------------

/// parse message and generate log records
pub fn generate_json_records<'a>(
    topic_name: &String,
    response_partitions: &Vec<FetchablePartitionResponse<DefaultRecords>>,
    suppress: bool,
) -> Vec<Value> {
    let mut json_records: Vec<Value> = vec![];

    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            print_cli_err!(err);
            continue;
        }

        let mut new_records = partition_to_json_records(&r_partition, suppress);
        json_records.append(&mut new_records);
    }

    json_records
}

/// Traverse all partition batches and parse records to json format
pub fn partition_to_json_records(
    partition: &FetchablePartitionResponse<DefaultRecords>,
    suppress: bool,
) -> Vec<Value> {
    let mut json_records: Vec<Value> = vec![];

    // convert all batches to json records
    for batch in &partition.records.batches {
        for record in &batch.records {
            if let Some(batch_record) = record.get_value().inner_value_ref() {
                match serde_json::from_slice(&batch_record) {
                    Ok(value) => json_records.push(value),
                    Err(_) => {
                        if !suppress {
                            json_records.push(serde_json::json!({
                                "error": record.get_value().describe()
                            }));
                        }
                    }
                }
            }
        }
    }

    json_records
}

/// Print json records to screen
fn print_json_records(records: &Vec<Value>) {
    println!("{},", serde_json::to_string_pretty(&records).unwrap());
}

// -----------------------------------
//  Text
// -----------------------------------

/// Print records in text format
pub fn print_text_records(
    topic_name: &String,
    response_partitions: &Vec<FetchablePartitionResponse<DefaultRecords>>,
    suppress: bool,
) {
    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            print_cli_err!(err);
            continue;
        }

        for batch in &r_partition.records.batches {
            for record in &batch.records {
                if record.get_value().inner_value_ref().is_some() {
                    if record.get_value().is_binary() {
                        if !suppress {
                            println!("{}", record.get_value().describe());
                        }
                    } else {
                        println!("{}", record.get_value());
                    }
                }
            }
        }
    }
}

// -----------------------------------
//  Binary
// -----------------------------------

/// parse message and generate partition records
pub fn print_binary_records(
    topic_name: &String,
    response_partitions: &Vec<FetchablePartitionResponse<DefaultRecords>>,
) {
    let mut printed = false;
    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            println!("{}", hex_dump_separator());
            print_cli_err!(err);
            printed = true;
            continue;
        }

        for batch in &r_partition.records.batches {
            for record in &batch.records {
                if let Some(batch_record) = record.get_value().inner_value_ref() {
                    println!("{}", hex_dump_separator());
                    println!("{}", bytes_to_hex_dump(&batch_record));
                    printed = true;
                }
            }
        }
    }
    if printed {
        println!("{}", hex_dump_separator());
    }
}

// -----------------------------------
//  Dynamic
// -----------------------------------

/// Print records based on their type
pub fn print_dynamic_records(
    topic_name: &String,
    response_partitions: &Vec<FetchablePartitionResponse<DefaultRecords>>,
) {
    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            print_cli_err!(err);
            continue;
        }

        for batch in &r_partition.records.batches {
            for record in &batch.records {
                if let Some(batch_record) = record.get_value().inner_value_ref() {
                    if record.get_value().is_binary() {
                        println!("{}", hex_dump_separator());
                        println!("{}", bytes_to_hex_dump(&batch_record));
                        println!("{}", hex_dump_separator());
                    } else {
                        println!("{}", record.get_value());
                    }
                }
            }
        }
    }
}

// -----------------------------------
//  Raw
// -----------------------------------

/// Print records in raw format
pub fn print_raw_records(
    topic_name: &String,
    response_partitions: &Vec<FetchablePartitionResponse<DefaultRecords>>,
) {
    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            print_cli_err!(err);
            continue;
        }

        for batch in &r_partition.records.batches {
            for record in &batch.records {
                if let Some(value) = record.get_value().inner_value_ref() {
                    let _ = io::stdout().write(value);
                }
            }
        }
    }
}

// -----------------------------------
//  Utilities
// -----------------------------------

/// If header has error, format and return
pub fn error_in_header(
    topic_name: &String,
    r_partition: &FetchablePartitionResponse<DefaultRecords>,
) -> Option<String> {
    if r_partition.error_code.is_error() {
        Some(format!(
            "topic '{}/{}': {}",
            topic_name,
            r_partition.partition_index,
            r_partition.error_code.to_sentence()
        ))
    } else {
        None
    }
}
