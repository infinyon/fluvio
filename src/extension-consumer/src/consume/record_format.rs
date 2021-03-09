//!
//! # Fluvio Fetch Logs
//!
//! Connects to server and fetches logs
//!

use fluvio_extension_common::{bytes_to_hex_dump, hex_dump_separator};

// -----------------------------------
//  JSON
// -----------------------------------

pub fn format_json(value: &[u8], suppress: bool) -> Option<String> {
    let maybe_json = match serde_json::from_slice(value) {
        Ok(value) => Some(value),
        Err(e) if !suppress => Some(serde_json::json!({
            "error": format!("{}", e),
        })),
        _ => None,
    };

    maybe_json.and_then(|json| serde_json::to_string(&json).ok())
}

// -----------------------------------
//  Text
// -----------------------------------

/// Print a single record in text format
pub fn format_text_record(record: &[u8], suppress: bool) -> String {
    if is_binary(record) && !suppress {
        format!("binary: ({} bytes)", record.len())
    } else {
        format!("{}", String::from_utf8_lossy(record))
    }
}

// -----------------------------------
//  Binary
// -----------------------------------

/// parse message and generate partition records
pub fn format_binary_record(record: &[u8]) -> String {
    let mut out = String::new();
    out.push_str(&bytes_to_hex_dump(record));
    out.push_str(&hex_dump_separator());
    out
}

// -----------------------------------
//  Dynamic
// -----------------------------------

/// Print records based on their type
pub fn format_dynamic_record(record: &[u8]) -> String {
    if is_binary(record) {
        format_binary_record(record)
    } else {
        format!("{}", String::from_utf8_lossy(record))
    }
}

// -----------------------------------
//  Raw
// -----------------------------------

/// Print records in raw format
pub fn format_raw_record(record: &[u8]) -> String {
    String::from_utf8_lossy(record).to_string()
}

// -----------------------------------
//  Utilities
// -----------------------------------

fn is_binary(bytes: &[u8]) -> bool {
    use content_inspector::{inspect, ContentType};
    matches!(inspect(bytes), ContentType::BINARY)
}
