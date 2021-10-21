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
//  Table
// -----------------------------------

// TODO: This will eventually read for alternative formatting
/// Print records in table format
pub fn format_table_record(record: &[u8]) -> String {
    use prettytable::{Row, cell};

    //let mut hashmap : std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();

    let maybe_json: serde_json::Value = match serde_json::from_slice(record) {
        Ok(value) => value,
        Err(_e) => panic!("Value not json"),
    };

    let obj = maybe_json.as_object().unwrap();

    // This is the case where we don't provide any table info. We want to print a table w/ all top-level keys as headers
    // Think about how we might only select specific keys
    let keys_str: Vec<String> = obj.keys().map(|k| k.to_string()).collect();

    // String
    // Boolean
    // Int
    // Float
    // Object should probably be printed as json str
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
    let entries: Row = Row::new(values_str.iter().map(|v| cell!(v.to_owned())).collect());

    // Print the table
    let mut t_print = Vec::new();
    // TODO: Don't print the headers in this table display
    t_print.push(header);
    t_print.push(entries);
    let _ = prettytable::Table::init(t_print).printstd();
    format!("")
}
// -----------------------------------
//  Utilities
// -----------------------------------

fn is_binary(bytes: &[u8]) -> bool {
    use content_inspector::{inspect, ContentType};
    matches!(inspect(bytes), ContentType::BINARY)
}

mod output {

    //!
    //! # Fluvio SC - output processing
    //!

    use prettytable::Row;
    use prettytable::row;
    use prettytable::Cell;
    use prettytable::cell;
    use prettytable::format::Alignment;
    use tracing::debug;
    use serde::Serialize;
    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;

    use fluvio::metadata::objects::Metadata;
    use fluvio_controlplane_metadata::table::TableSpec;

    use crate::CliError;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListTables(Vec<Metadata<TableSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format Table list
    pub fn tables_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_tables: Vec<Metadata<TableSpec>>,
        output_type: OutputType,
    ) -> Result<(), CliError> {
        debug!("tables: {:#?}", list_tables);

        //if !list_tables.is_empty() {
        //    let tables = ListTables(list_tables);
        //    out.render_list(&tables, output_type)?;
        //    Ok(())
        //} else {
        //    t_println!(out, "no tables");
        //    Ok(())
        //}
        Ok(())
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListTables {
        // This needs to be flexible to the keys, or the table spec
        /// table header implementation
        fn header(&self) -> Row {
            row!["NAME", "STATUS",]
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        // This needs to be flexible to the keys, or the table spec
        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|r| {
                    let _spec = &r.spec;
                    Row::new(vec![
                        Cell::new_align(&r.name, Alignment::RIGHT),
                        Cell::new_align(&r.status.to_string(), Alignment::RIGHT),
                    ])
                })
                .collect()
        }
    }
}
