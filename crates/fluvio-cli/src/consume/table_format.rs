// -----------------------------------
//  Basic Table
// -----------------------------------

///// Print records in text-based table format
//pub fn print_table_record(record: &[u8], count: i32) -> String {
//    use prettytable::{Row, cell, Cell, Slice};
//    use prettytable::format::{self, FormatBuilder};
//
//    let maybe_json: serde_json::Value = match serde_json::from_slice(record) {
//        Ok(value) => value,
//        Err(_e) => panic!("Value not json"),
//    };
//
//    let obj = maybe_json.as_object().unwrap();
//
//    // This is the case where we don't provide any table info. We want to print a table w/ all top-level keys as headers
//    // Think about how we might only select specific keys
//    let keys_str: Vec<String> = obj.keys().map(|k| k.to_string()).collect();
//
//    // serde_json's Value::String() gets wrapped in quotes if we use `to_string()`
//    let values_str: Vec<String> = obj
//        .values()
//        .map(|v| {
//            if v.is_string() {
//                v.as_str()
//                    .expect("Value not representable as str")
//                    .to_string()
//            } else {
//                v.to_string()
//            }
//        })
//        .collect();
//
//    let header: Row = Row::new(keys_str.iter().map(|k| cell!(k.to_owned())).collect());
//    let entries: Row = Row::new(values_str.iter().map(|v| Cell::new(v)).collect());
//
//    // Print the table
//    let t_print = vec![header, entries];
//
//    let mut table = prettytable::Table::init(t_print);
//
//    let base_format: FormatBuilder = (*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR).into();
//    let table_format = base_format;
//    table.set_format(table_format.build());
//
//    // FIXME: Live display of table data easily misaligns column widths
//    // if there is a length diff between the header and the data
//    // The rows after the first (count == 0) don't line up with the header
//    // prettytable might not support the live display use-case we want
//    if count == 0 {
//        table.printstd();
//    } else {
//        let slice = table.slice(1..);
//        slice.printstd();
//    }
//    format!("")
//}

// -----------------------------------
//  Fancy Table
// -----------------------------------

// -----------------------------------
//  Utilities
// -----------------------------------
