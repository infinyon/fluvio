//! A SmartStream example showing how to filter records based on a JSON value
//!
//! In this example, we use `serde` to deserialize our Records in a JSON
//! format in order to examine the fields of our data. We can imagine the
//! data in our topic as being application logs from some server or other
//! program, and that we want our SmartStream to keep only those log
//! messages that are tagged as "info", "warn", or "error".
//!
//! To get more concrete, let's say we have a Topic called "application-logs":
//!
//! ```text
//! $ fluvio topic create application-logs
//! ```
//!
//! And that the data in this topic is JSON-formatted log messages, with
//! a "level" field and a "message" field. If we consume these records with
//! no SmartStreams enabled, we might see records like the following:
//!
//! ```text
//! $ fluvio consume application-logs -B
//! {"level":"info","message":"Server listening on 0.0.0.0:8000"}
//! {"level":"info","message":"Accepted incoming connection"}
//! {"level":"debug","message":"Deserializing request from client"}
//! {"level":"debug","message":"Client request deserialized"}
//! {"level":"debug","message":"Connecting to database"}
//! {"level":"warn","message":"Client dropped connnection"}
//! {"level":"info","message":"Accepted incoming connection"}
//! {"level":"debug","message":"Deserializing request from client"}
//! {"level":"debug","message":"Client request deserialized"}
//! {"level":"debug","message":"Connecting to database"}
//! {"level":"error","message":"Unable to connect to database"}
//! ```
//!
//! All of the debug messages in this stream are causing a lot of noise.
//! We can use this SmartStream to filter out any records that do not
//! have a log level of "info", "warn", or "error".
//!
//! ```text
//! $ cargo build --release -p fluvio-smartstream-filter-json
//! $ fluvio consume application-logs -B --smart-stream=target/wasm32-unknown-unknown/release/fluvio-smartstream-filter-json.wasm
//! {"level":"info","message":"Server listening on 0.0.0.0:8000"}
//! {"level":"info","message":"Accepted incoming connection"}
//! {"level":"warn","message":"Client dropped connnection"}
//! {"level":"info","message":"Accepted incoming connection"}
//! {"level":"error","message":"Unable to connect to database"}
//! ```

use fluvio_smartstream::{smartstream, Record, Result};

#[derive(PartialEq, Eq, PartialOrd, Ord, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(serde::Deserialize)]
struct StructuredLog {
    level: LogLevel,
    #[serde(rename = "message")]
    _message: String,
}

#[smartstream(filter)]
pub fn filter_log_level(record: &Record) -> Result<bool> {
    let log = serde_json::from_slice::<StructuredLog>(record.value.as_ref())?;
    Ok(log.level > LogLevel::Debug)
}
