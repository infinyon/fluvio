//! This SmartModule takes JSON arrays as inputs and returns the values in those arrays as output.
//!
//! Sometimes we want to take a composite value like a JSON array and break it down into
//! it's component pieces, i.e. the values inside the array. We can do this using a
//! SmartModule Flatmap.
//!
//! In this example, we'll take a stream of JSON arrays as input, and we'll return a stream
//! of all the child _values_ that were in those arrays as output.
//!
//! To test this SmartModule, set up a test Topic:
//!
//! ```text
//! $ fluvio topic create array-map-array
//! ```
//!
//! Produce some valid JSON arrays as input:
//!
//! ```text
//! $ fluvio produce array-map-array
//! > ["Apple", "Banana", Cranberry"]
//! Ok!
//! > ^C
//! ```
//!
//! Then, make sure you have compiled the SmartModule examples, and run the consumer:
//!
//! ```text
//! $ cd smartmodule/examples
//! $ cargo build --release
//! $ fluvio consume array-map-array -B --array-map=target/wasm32-unknown-unknown/release/fluvio_wasm_array_map_array.wasm
//! "Apple"
//! "Banana"
//! "Cranberry"
//! ```

use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(array_map)]
pub fn array_map(record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    // Deserialize a JSON array with any kind of values inside
    let array: Vec<serde_json::Value> = serde_json::from_slice(record.value.as_ref())?;

    // Convert each JSON value from the array back into a JSON string
    let strings: Vec<String> = array
        .into_iter()
        .map(|value| serde_json::to_string(&value))
        .collect::<core::result::Result<_, _>>()?;

    // Create one record from each JSON string to send
    let records: Vec<(Option<RecordData>, RecordData)> = strings
        .into_iter()
        .map(|s| (None, RecordData::from(s)))
        .collect();
    Ok(records)
}
