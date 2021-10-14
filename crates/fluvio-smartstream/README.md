# Fluvio SmartStreams

This crate provides types and macros for creating custom SmartStreams,
which are WebAssembly modules that may be used to modify the behavior
of Fluvio streams to consumers. The currently supported SmartStream
types are `filter`s, which may describe records to keep in or discard
from a stream.

## Writing SmartStreams

> See the `examples` directory for full examples.

All SmartStreams require adding `crate-type = ['cdylib']` to the Cargo.toml.
For a quick setup using `cargo-generate`, see [the SmartStream template].

[the SmartStream template]: https://github.com/infinyon/fluvio-smartstream-template

```toml
[package]
name = "fluvio-wasm-filter"
version = "0.1.0"
authors = ["Fluvio Contributors <team@fluvio.io>"]
edition = "2018"

[lib]
crate-type = ['cdylib']

[dependencies]
fluvio-smartstream = "0.2.0"
```

### Filtering

For filtering, write your smartstream using `#[smartstream(filter)]` on your
top-level function. Consider this the "main" function of your SmartStream.

```ignore
use fluvio_smartstream::{smartstream, Record, Result};

#[smartstream(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(string.contains('a'))
}
```

This filter will keep only records whose contents contain the letter `a`.

### Mapping

Mapping functions use `#[smartstream(map)]`, and are also a top-level entrypoint.

```ignore
use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();

    let string = std::str::from_utf8(record.value.as_ref())?;
    let int = string.parse::<i32>()?;
    let value = (int * 2).to_string();

    Ok((key, value.into()))
}
```

This SmartStream will read each input Record as an integer (`i32`), then multiply it by 2.

### Aggregate

Aggregate functions are a way to combine the data from many input records.
Each time the aggregate function is called, it receives an "accumulated" value
as well as the value of the current record in the stream, and is expected to
combine the accumulator with the value to produce a new accumulator. This new
accumulator value will be passed to the next invocation of `aggregate` with
the next record value. The resulting stream of values is the output accumulator
from each step.

```ignore
use fluvio_smartstream::{smartstream, Result, Record, RecordData};

#[smartstream(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &Record) -> Result<RecordData> {
    let mut acc = String::from_utf8(accumulator.as_ref().to_vec())?;
    let next = std::str::from_utf8(current.value.as_ref())?;
    acc.push_str(next);
    Ok(acc.into())
}
```

This SmartStream reads each record as a string and appends it to the accumulator string.

### Flatmap

Flatmap functions are used to take one input record and create zero to many output records.
This can be used to chop up input records that logically represent more than one data point
and turn them into independent records. Below is an example where we take JSON arrays and
convert them into a stream of the inner JSON objects.

```ignore
use fluvio_smartstream::{smartstream, Result, Record, RecordData};

#[smartstream(flat_map)]
pub fn flatmap(record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    // Read the input record as a JSON array
    let array = serde_json::from_slice::<Vec<serde_json::Value>>(record.value.as_ref())?;
    
    // Convert each individual value from the array into its own JSON string
    let strings = array
        .into_iter()
        .map(|value| serde_json::to_string(&value))
        .collect::<core::result::Result<Vec<String>, _>>()?;
        
    // Return a list of records to be flattened into the output stream
    let kvs = strings
        .into_iter()
        .map(|s| (None, RecordData::from(s)))
        .collect::<Vec<_>>();
    Ok(kvs)
}
```

## License

This project is licensed under the [Apache license](LICENSE-APACHE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Fluvio by you, shall be licensed as Apache, without any additional
terms or conditions.
