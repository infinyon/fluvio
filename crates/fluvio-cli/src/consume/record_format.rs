//!
//! # Fluvio Fetch Logs
//!
//! Connects to server and fetches logs
//!

use fluvio_extension_common::{bytes_to_hex_dump, hex_dump_separator};
use super::TableModel;

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
//  Table (basic table)
// -----------------------------------

/// Structure json data into table row
/// Print table header if `print_header` is true
/// Rows may not stay aligned with table header
pub fn format_basic_table_record(record: &[u8], print_header: bool) -> Option<String> {
    use prettytable::{Row, cell, Cell, Slice};
    use prettytable::format::{self, FormatBuilder};

    let maybe_json: serde_json::Value = match serde_json::from_slice(record) {
        Ok(value) => value,
        Err(e) => {
            println!("error parsing record as json: {}", e);
            return None;
        }
    };

    let obj = maybe_json.as_object().unwrap();

    // This is the case where we don't provide any table info. We want to print a table w/ all top-level keys as headers
    // Think about how we might only select specific keys
    let keys_str: Vec<String> = obj.keys().map(|k| k.to_string()).collect();

    // serde_json's Value::String() gets wrapped in quotes if we use `to_string()`
    let values_str: Vec<String> = obj
        .values()
        .map(|v| {
            if v.is_string() {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else {
                    println!("error: Value in json not representable as str");
                    String::new()
                }
            } else {
                v.to_string()
            }
        })
        .collect();

    let header: Row = Row::new(keys_str.iter().map(|k| cell!(k.to_owned())).collect());
    let entries: Row = Row::new(values_str.iter().map(|v| Cell::new(v)).collect());

    // Print the table
    let t_print = vec![header, entries];

    let mut table = prettytable::Table::init(t_print);

    let base_format: FormatBuilder = (*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR).into();
    let table_format = base_format;
    table.set_format(table_format.build());

    let mut out = Vec::new();
    let res = if print_header {
        table.print(&mut out)
    } else {
        let slice = table.slice(1..);
        slice.print(&mut out)
    };

    if res.is_ok() {
        Some(String::from_utf8_lossy(&out).trim_end().to_string())
    } else {
        None
    }
}

// -----------------------------------
//  Full Table (fullscreen interactive table)
// -----------------------------------

/// Updates the TableModel used to render the TUI table during `TableModel::render()`
/// Attempts to update relevant rows, but appends to table if the primary key doesn't exist
/// Returned String is not intended to be used
pub fn format_fancy_table_record(record: &[u8], table_model: &mut TableModel) -> Option<String> {
    let maybe_json: serde_json::Value = match serde_json::from_slice(record) {
        Ok(value) => value,
        Err(e) => {
            println!("error parsing record as json: {}", e);
            return None;
        }
    };

    let obj = maybe_json.as_object().unwrap();
    let keys_str: Vec<String> = obj.keys().map(|k| k.to_string()).collect();

    // serde_json's Value::String() gets wrapped in quotes if we use `to_string()`
    let values_str: Vec<String> = obj
        .values()
        .map(|v| {
            if v.is_string() {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else {
                    println!("error: Value in json not representable as str");
                    String::new()
                }
            } else {
                v.to_string()
            }
        })
        .collect();

    let header = keys_str;
    if table_model.update_header(header).is_err() {
        println!("Unable to set table headers")
    }

    if table_model.update_row(values_str).is_err() {
        println!("Unable to update table row");
    }

    None
}

// -----------------------------------
//  Utilities
// -----------------------------------

fn is_binary(bytes: &[u8]) -> bool {
    use content_inspector::{inspect, ContentType};
    matches!(inspect(bytes), ContentType::BINARY)
}
