//!
//! # Fluvio Fetch Logs
//!
//! Connects to server and fetches logs
//!

use fluvio_extension_common::{bytes_to_hex_dump, hex_dump_separator};
use super::TableModel;

use std::io::Stdout;
use tui::{backend::CrosstermBackend, Terminal};
use crossterm::{
    event::DisableMouseCapture,
    execute,
    terminal::{disable_raw_mode, LeaveAlternateScreen},
};

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
pub fn format_basic_table_record(record: &[u8], print_header: bool) -> String {
    use prettytable::{Row, cell, Cell, Slice};
    use prettytable::format::{self, FormatBuilder};

    let maybe_json: serde_json::Value = match serde_json::from_slice(record) {
        Ok(value) => value,
        Err(_e) => panic!("Value not json"),
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
                v.as_str()
                    .expect("Value not representable as str")
                    .to_string()
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
    if print_header {
        table.print(&mut out).expect("Unable to print table");
    } else {
        let slice = table.slice(1..);
        slice.print(&mut out).expect("Unable to print row");
    }

    format!("{}", String::from_utf8_lossy(&out))
}

// -----------------------------------
//  Full Table (fullscreen interactive table)
// -----------------------------------

/// Updates the TableModel used to render the TUI table during `TableModel::render()`
/// Attempts to update relevant rows, but appends to table if the primary key doesn't exist
/// Returned String is not intended to be used
pub fn format_fancy_table_record(
    record: &[u8],
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    table_model: &mut TableModel,
) -> String {
    let maybe_json: serde_json::Value = match serde_json::from_slice(record) {
        Ok(value) => value,
        Err(_e) => {
            // restore terminal
            disable_raw_mode().expect("Disabling raw mode failed");
            execute!(
                terminal.backend_mut(),
                LeaveAlternateScreen,
                DisableMouseCapture
            )
            .expect("Failed to leave alternate screen");
            terminal.show_cursor().expect("Show terminal cursor failed");

            panic!("Value not json")
        }
    };

    let obj = maybe_json.as_object().unwrap();
    let keys_str: Vec<String> = obj.keys().map(|k| k.to_string()).collect();

    // serde_json's Value::String() gets wrapped in quotes if we use `to_string()`
    let values_str: Vec<String> = obj
        .values()
        .map(|v| {
            if v.is_string() {
                v.as_str()
                    .expect("Value not representable as str")
                    .to_string()
            } else {
                v.to_string()
            }
        })
        .collect();

    let header = keys_str;
    table_model
        .update_header(header)
        .expect("Unable to set table headers");
    table_model
        .update_row(values_str)
        .expect("Unable to update table row");

    String::new()
}

// -----------------------------------
//  Utilities
// -----------------------------------

fn is_binary(bytes: &[u8]) -> bool {
    use content_inspector::{inspect, ContentType};
    matches!(inspect(bytes), ContentType::BINARY)
}
