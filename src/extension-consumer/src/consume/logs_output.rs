//!
//! # Fluvio Fetch Logs
//!
//! Connects to server and fetches logs
//!

use serde_json::Value;

use crate::common::output::OutputError;
use crate::consume::ConsumeLogOpt;
use super::ConsumeOutputType;
use fluvio_extension_common::{bytes_to_hex_dump, hex_dump_separator};

/// Process fetch topic response based on output type
pub fn print_record(record: &[u8], config: &ConsumeLogOpt) -> Result<(), OutputError> {
    match config.output {
        ConsumeOutputType::json => {
            if let Some(json) = record_to_json(record, config.suppress_unknown) {
                print_json_record(&json);
            }
        }
        ConsumeOutputType::text => {
            print_text_record(record, config.suppress_unknown);
        }
        ConsumeOutputType::binary => {
            print_binary_record(record);
        }
        ConsumeOutputType::dynamic => {
            print_dynamic_record(record);
        }
        ConsumeOutputType::raw => {
            print_raw_record(record);
        }
    }

    Ok(())
}

// -----------------------------------
//  JSON
// -----------------------------------

pub fn record_to_json(record: &[u8], suppress: bool) -> Option<Value> {
    match serde_json::from_slice(record) {
        Ok(value) => Some(value),
        Err(e) if !suppress => Some(serde_json::json!({
            "error": format!("{}", e),
        })),
        _ => None,
    }
}

/// Print json records to screen
fn print_json_record(record: &Value) {
    println!("{},", serde_json::to_string_pretty(&record).unwrap());
}

// -----------------------------------
//  Text
// -----------------------------------

/// Print a single record in text format
pub fn print_text_record(record: &[u8], suppress: bool) {
    if is_binary(record) && !suppress {
        println!("binary: ({} bytes)", record.len());
    } else {
        println!("{}", String::from_utf8_lossy(record));
    }
}

// -----------------------------------
//  Binary
// -----------------------------------

/// parse message and generate partition records
pub fn print_binary_record(record: &[u8]) {
    println!("{}", bytes_to_hex_dump(record));
    println!("{}", hex_dump_separator());
}

// -----------------------------------
//  Dynamic
// -----------------------------------

/// Print records based on their type
pub fn print_dynamic_record(record: &[u8]) {
    if is_binary(record) {
        print_binary_record(record);
    } else {
        println!("{}", String::from_utf8_lossy(record));
    }
}

// -----------------------------------
//  Raw
// -----------------------------------

/// Print records in raw format
pub fn print_raw_record(record: &[u8]) {
    let str_value = std::str::from_utf8(record).unwrap();
    println!("{}", str_value);
}

// -----------------------------------
//  Utilities
// -----------------------------------

fn is_binary(bytes: &[u8]) -> bool {
    use content_inspector::{inspect, ContentType};
    matches!(inspect(bytes), ContentType::BINARY)
}
