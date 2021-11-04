//!
//! # Fluvio Fetch Logs
//!
//! Connects to server and fetches logs
//!

use fluvio_extension_common::{bytes_to_hex_dump, hex_dump_separator};

use std::io::Stdout;
use tui::Terminal;
use tui::backend::CrosstermBackend;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
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
//  Table
// -----------------------------------

// I don't know if I need this yet. This could be a place to structure values in view.
pub fn _format_table_record(_record: &[u8]) -> String {
    unimplemented!();
}

// This will take a full screen buffer
/// Print records in table format
pub fn print_table_record(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    record: &[u8],
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

    //terminal.draw()

    //let header: Row = Row::new(keys_str.iter().map(|k| cell!(k.to_owned())).collect());
    //let entries: Row = Row::new(values_str.iter().map(|v| Cell::new(v)).collect());

    //// Print the table
    //let t_print = vec![header, entries];

    //let mut table = prettytable::Table::init(t_print);

    //let base_format: FormatBuilder = (*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR).into();
    //let table_format = base_format;
    //table.set_format(table_format.build());

    //// FIXME: Live display of table data easily misaligns column widths
    //// if there is a length diff between the header and the data
    //// The rows after the first (count == 0) don't line up with the header
    //// prettytable might not support the live display use-case we want
    //if count == 0 {
    //    table.printstd();
    //} else {
    //    let slice = table.slice(1..);
    //    slice.printstd();
    //}
    format!("")
}
// -----------------------------------
//  Utilities
// -----------------------------------

fn is_binary(bytes: &[u8]) -> bool {
    use content_inspector::{inspect, ContentType};
    matches!(inspect(bytes), ContentType::BINARY)
}
