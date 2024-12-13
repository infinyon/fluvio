# Fluvio SmartModules

This crate provides types and macros for creating custom SmartModules,
which are WebAssembly modules that may be used to modify the behavior
of Fluvio streams to consumers. The currently supported SmartModule
types are `filter`s, which may describe records to keep in or discard
from a stream.

## Writing SmartModules

> See the `examples` directory for full examples.

All SmartModules require adding `crate-type = ['cdylib']` to the Cargo.toml.
For a quick setup using `cargo-generate`, see [the SmartModule template].

[the SmartModule template]: https://github.com/infinyon/fluvio-smartmodule-template

```toml
[package]
name = "fluvio-wasm-filter"
version = "0.1.0"
authors = ["Fluvio Contributors <team@fluvio.io>"]
edition = "2021"

[lib]
crate-type = ['cdylib']

[dependencies]
fluvio-smartmodule = "0.8.0"
```

### Filtering

For filtering, write your smartmodule using `#[smartmodule(filter)]` on your
top-level function. Consider this the "main" function of your SmartModule.

```rust
use fluvio_smartmodule::{smartmodule, SmartModuleRecord, Result};

#[smartmodule(filter)]
pub fn filter(record: &SmartModuleRecord) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(string.contains('a'))
}
```

This filter will keep only records whose contents contain the letter `a`.

### Mapping

Mapping functions use `#[smartmodule(map)]`, and are also a top-level entrypoint.

```rust
use fluvio_smartmodule::{smartmodule, SmartModuleRecord, RecordData, Result};

#[smartmodule(map)]
pub fn map(record: &SmartModuleRecord) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();

    let string = std::str::from_utf8(record.value.as_ref())?;
    let int = string.parse::<i32>()?;
    let value = (int * 2).to_string();

    Ok((key, value.into()))
}
```

This SmartModule will read each input Record as an integer (`i32`), then multiply it by 2.

### Aggregate

Aggregate functions are a way to combine the data from many input records.
Each time the aggregate function is called, it receives an "accumulated" value
as well as the value of the current record in the stream, and is expected to
combine the accumulator with the value to produce a new accumulator. This new
accumulator value will be passed to the next invocation of `aggregate` with
the next record value. The resulting stream of values is the output accumulator
from each step.

```rust
use fluvio_smartmodule::{smartmodule, Result, SmartModuleRecord, RecordData};

#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &SmartModuleRecord) -> Result<RecordData> {
    let mut acc = String::from_utf8(accumulator.as_ref().to_vec())?;
    let next = std::str::from_utf8(current.value.as_ref())?;
    acc.push_str(next);
    Ok(acc.into())
}
```

This SmartModule reads each record as a string and appends it to the accumulator string.

### ArrayMap

ArrayMap functions are used to take one input record and create zero to many output records.
This can be used to chop up input records that logically represent more than one data point
and turn them into independent records. Below is an example where we take JSON arrays and
convert them into a stream of the inner JSON objects.

```ignore
use fluvio_smartmodule::{smartmodule, SmartModuleRecord, RecordData, Result};

#[smartmodule(array_map)]
pub fn array_map(record: &SmartModuleRecord) -> Result<Vec<(Option<RecordData>, RecordData)>> {
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
```

### Init

Init functions are optional but serve to configure any state the SmartModule requires at the beginning of its execution. Could be helpful for preparing the SmartModule's operational context. The example below demonstrates an init function that sets a key for the SmartModule to use:

```ignore
use std::sync::OnceLock;

use fluvio_smartmodule::{
    smartmodule, Result, eyre,
    dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInitError},
};

static CRITERIA: OnceLock<String> = OnceLock::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(key) = params.get("key") {
        CRITERIA
            .set(key.clone())
            .map_err(|err| eyre!("failed setting key: {:#?}", err))
    } else {
        Err(SmartModuleInitError::MissingParam("key".to_string()).into())
    }
}
```

## License

This project is licensed under the [Apache license](LICENSE-APACHE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Fluvio by you, shall be licensed as Apache, without any additional
terms or conditions.
