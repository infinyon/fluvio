//!
//! # Fluvio Fetch Logs
//!
//! Connects to server and fetches logs
//!

use log::debug;
use serde_json;
use serde_json::Value;

use flv_client::kf::api::RecordSet;
use flv_client::kf::message::fetch::FetchablePartitionResponse;

use crate::error::CliError;
use crate::common::{bytes_to_hex_dump, hex_dump_separator};
use crate::Terminal;
use crate::t_println;
use crate::t_print_cli_err;

use super::ConsumeLogConfig;
use super::ConsumeOutputType;

/// Process fetch topic response based on output type
pub async fn process_fetch_topic_response<O>(
    out: std::sync::Arc<O>,
    topic: &str,
    response: FetchablePartitionResponse<RecordSet>,
    config: &ConsumeLogConfig,
) -> Result<(), CliError>
where
    O: Terminal,
{
    let partition_res = vec![response];

    match config.output {
        ConsumeOutputType::json => {
            let records =
                generate_json_records(out.clone(), topic, &partition_res, config.suppress_unknown);
            print_json_records(out, &records);
        }
        ConsumeOutputType::text => {
            print_text_records(out, topic, &partition_res, config.suppress_unknown);
        }
        ConsumeOutputType::binary => {
            print_binary_records(out, topic, &partition_res);
        }
        ConsumeOutputType::dynamic => {
            print_dynamic_records(out, topic, &partition_res);
        }
        ConsumeOutputType::raw => {
            print_raw_records(out, topic, &partition_res);
        }
    }

    Ok(())
}

// -----------------------------------
//  JSON
// -----------------------------------

/// parse message and generate log records
pub fn generate_json_records<O>(
    out: std::sync::Arc<O>,
    topic_name: &str,
    response_partitions: &Vec<FetchablePartitionResponse<RecordSet>>,
    suppress: bool,
) -> Vec<Value>
where
    O: Terminal,
{
    let mut json_records: Vec<Value> = vec![];

    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            t_print_cli_err!(out, err);
            continue;
        }

        let mut new_records = partition_to_json_records(&r_partition, suppress);
        json_records.append(&mut new_records);
    }

    json_records
}

/// Traverse all partition batches and parse records to json format
pub fn partition_to_json_records(
    partition: &FetchablePartitionResponse<RecordSet>,
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
fn print_json_records<O>(out: std::sync::Arc<O>, records: &Vec<Value>)
where
    O: Terminal,
{
    t_println!(out, "{},", serde_json::to_string_pretty(&records).unwrap());
}

// -----------------------------------
//  Text
// -----------------------------------

/// Print records in text format
pub fn print_text_records<O>(
    out: std::sync::Arc<O>,
    topic_name: &str,
    response_partitions: &Vec<FetchablePartitionResponse<RecordSet>>,
    suppress: bool,
) where
    O: Terminal,
{
    debug!("processing text record: {:#?}", response_partitions);

    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            t_print_cli_err!(out, err);
            continue;
        }

        for batch in &r_partition.records.batches {
            for record in &batch.records {
                if record.get_value().inner_value_ref().is_some() {
                    if record.get_value().is_binary() {
                        if !suppress {
                            t_println!(out, "{}", record.get_value().describe());
                        }
                    } else {
                        t_println!(out, "{}", record.get_value());
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
pub fn print_binary_records<O>(
    out: std::sync::Arc<O>,
    topic_name: &str,
    response_partitions: &Vec<FetchablePartitionResponse<RecordSet>>,
) where
    O: Terminal,
{
    debug!(
        "printing out binary records: {} records: {}",
        topic_name,
        response_partitions.len()
    );
    let mut printed = false;
    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            t_println!(out, "{}", hex_dump_separator());
            t_print_cli_err!(out, err);
            printed = true;
            continue;
        }

        for batch in &r_partition.records.batches {
            for record in &batch.records {
                if let Some(batch_record) = record.get_value().inner_value_ref() {
                    t_println!(out, "{}", hex_dump_separator());
                    t_println!(out, "{}", bytes_to_hex_dump(&batch_record));
                    printed = true;
                }
            }
        }
    }
    if printed {
        t_println!(out, "{}", hex_dump_separator());
    }
}

// -----------------------------------
//  Dynamic
// -----------------------------------

/// Print records based on their type
pub fn print_dynamic_records<O>(
    out: std::sync::Arc<O>,
    topic_name: &str,
    response_partitions: &Vec<FetchablePartitionResponse<RecordSet>>,
) where
    O: Terminal,
{
    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            t_print_cli_err!(out, err);
            continue;
        }

        for batch in &r_partition.records.batches {
            for record in &batch.records {
                if let Some(batch_record) = record.get_value().inner_value_ref() {
                    if record.get_value().is_binary() {
                        t_println!(out, "{}", hex_dump_separator());
                        t_println!(out, "{}", bytes_to_hex_dump(&batch_record));
                        t_println!(out, "{}", hex_dump_separator());
                    } else {
                        t_println!(out, "{}", record.get_value());
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
pub fn print_raw_records<O>(
    out: std::sync::Arc<O>,
    topic_name: &str,
    response_partitions: &Vec<FetchablePartitionResponse<RecordSet>>,
) where
    O: Terminal,
{
    for r_partition in response_partitions {
        if let Some(err) = error_in_header(topic_name, r_partition) {
            t_print_cli_err!(out, err);
            continue;
        }

        for batch in &r_partition.records.batches {
            for record in &batch.records {
                if let Some(value) = record.get_value().inner_value_ref() {
                    let str_value = std::str::from_utf8(value).unwrap();
                    t_println!(out, "{}", str_value);
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
    topic_name: &str,
    r_partition: &FetchablePartitionResponse<RecordSet>,
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
