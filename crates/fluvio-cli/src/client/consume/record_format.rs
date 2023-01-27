//!
//! # Fluvio Fetch Logs
//!
//! Connects to server and fetches logs
//!
use std::collections::BTreeMap;

use comfy_table::Table;
use anyhow::{anyhow, Result};

use fluvio::{metadata::tableformat::TableFormatColumnConfig};
use fluvio_extension_common::{bytes_to_hex_dump, hex_dump_separator};

use super::TableModel;

// -----------------------------------
//  JSON
// -----------------------------------

pub fn format_json(value: &[u8], suppress: bool) -> Option<String> {
    let maybe_json = match serde_json::from_slice(value) {
        Ok(value) => Some(value),
        Err(e) if !suppress => Some(serde_json::json!({
            "error": format!("{e}"),
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
    use comfy_table::{Row, Cell};

    let maybe_json: serde_json::Value = match serde_json::from_slice(record) {
        Ok(value) => value,
        Err(e) => {
            println!("error parsing record as json: {e}");
            return None;
        }
    };

    let obj = if let Some(obj) = maybe_json.as_object() {
        obj
    } else {
        println!("error: Unable to parse json as object map");
        return None;
    };

    // This is the case where we don't provide any table info. We want to print a table w/ all top-level keys as headers
    // Think about how we might only select specific keys
    let keys_str = obj.keys().map(|k| k.to_string());

    // serde_json's Value::String() gets wrapped in quotes if we use `to_string()`
    let values_str = obj.values().map(|v| {
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
    });

    let header: Row = Row::from(keys_str.into_iter().map(Cell::new).collect::<Vec<_>>());

    let entries: Row = Row::from(values_str.into_iter().map(Cell::new).collect::<Vec<_>>());

    let mut table = Table::new();
    table.set_header(header);
    table.add_row(entries);

    let out: Vec<String> = if print_header {
        table.lines().collect()
    } else {
        table.lines().skip(1).collect()
    };

    table.load_preset(comfy_table::presets::NOTHING);

    Some(out.join("\n"))
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
            println!("error parsing record as json: {e}");
            return None;
        }
    };

    // Handle updates as objects or list of objects
    match maybe_json {
        serde_json::Value::Object(json_obj) => {
            update_table_row(table_model, json_obj).ok()?;
        }
        serde_json::Value::Array(vec_obj) => {
            let json_array = flatten_json_array_updates(vec_obj).ok()?;
            for json_obj in json_array {
                update_table_row(table_model, json_obj).ok()?;
            }
        }

        _ => {
            println!("error: Unable to parse json as object map or array of objects");
            return None;
        }
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

// Updates the table model based on a single json object
fn update_table_row(
    table_model: &mut TableModel,
    object: serde_json::Map<String, serde_json::Value>,
) -> Result<()> {
    //if let serde_json::Value::Object(obj) = object {
    let mut new_data: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in object {
        let key = k.to_string();
        let value = if v.is_string() {
            if let Some(s) = v.as_str() {
                s.to_string()
            } else {
                println!("error: Value in json not representable as str");
                String::new()
            }
        } else {
            v.to_string()
        };

        new_data.insert(key, value);
    }

    // This only expected if the columns have not yet been defined
    // Typically the result is columns sorted alphabetically
    if table_model.columns().is_empty() {
        //println!("Columns empty");
        // Build a list of TableColumn
        let mut columns = vec![];

        for k in new_data.keys() {
            columns.push(TableFormatColumnConfig::new(k.to_string()))
        }

        if table_model.update_columns(columns).is_err() {
            println!("Unable to set table headers")
        }
    }

    // This will append if now primary keys or update rows based on primary key values
    if table_model.update_row(new_data).is_err() {
        println!("Unable to update table row");
    }
    Ok(())
}

// This is to handle if a list of table updates are passed in via JSON array
fn flatten_json_array_updates(
    maybe_json_array: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
    let mut vec_json_objs = Vec::new();
    // Check that array is all objects
    for value in maybe_json_array {
        if let serde_json::Value::Object(obj) = value {
            vec_json_objs.push(obj);
            continue;
        } else {
            return Err(anyhow!("Expected all values in json array to be objects"));
        }
    }
    Ok(vec_json_objs)
}
