//! This SmartModule takes JSON objects as inputs and returns key/value entries as output.
//!
//! JSON objects are made up of key/value entries, where the keys must be unique strings.
//! Consider the following JSON object:
//!
//! ```text
//! {
//!   "a": "Apple",
//!   "b": "Banana",
//!   "c": "Cranberry"
//! }
//! ```
//!
//! Another way to view this object is as an iterator over its key/value pairs:
//!
//! ```text
//! ...
//! ("a", "Apple")
//! ("b", "Banana")
//! ("c", "Cranberry")
//! ...
//! ```
//!
//! With this SmartModule, we use `#[smartmodule(array_map)]` to convert a stream
//! of JSON objects into a stream of all of the _children_ of those objects, using
//! the JSON object keys as the output record keys.
//!
//! To test this SmartModule, set up a test Topic:
//!
//! ```text
//! $ fluvio topic create array-map-object
//! ```
//!
//! Produce some valid JSON objects as input:
//!
//! ```text
//! $ fluvio produce array-map-object
//! > {"a": "Apple", "b": "Banana", "c": "Cranberry"}
//! Ok!
//! > ^C
//! ```
//!
//! Then, make sure you have compiled the SmartModule examples, and run the consumer:
//!
//! ```text
//! $ cd smartmodule/examples
//! $ cargo build --release
//! $ fluvio consume array-map-object -B --key-value --array-map=target/wasm32-unknown-unknown/release/fluvio_wasm_array_map_object.wasm
//! [a] "Apple"
//! [b] "Banana"
//! [c] "Cranberry"
//! ```

use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};
use serde_json::{Map, Value};

#[smartmodule(array_map)]
pub fn array_map(record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    // Deserialize a JSON object (Map) with any kind of values inside
    let object: Map<String, Value> = serde_json::from_slice(record.value.as_ref())?;

    // Convert each JSON value from the array back into a JSON string
    let key_value_strings: Vec<(&String, String)> = object
        .iter()
        .map(|(key, value)| serde_json::to_string(value).map(|value| (key, value)))
        .collect::<core::result::Result<_, _>>()?;

    // Create one record from each JSON string to send
    let key_value_records: Vec<(Option<RecordData>, RecordData)> = key_value_strings
        .into_iter()
        .map(|(key, value)| {
            (
                Some(RecordData::from(key.to_string())),
                RecordData::from(value),
            )
        })
        .collect();
    Ok(key_value_records)
}
