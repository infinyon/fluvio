# Fluvio SmartStreams

This crate provides types and macros for creating custom SmartStreams,
which are WebAssembly modules that may be used to modify the behavior
of Fluvio streams to consumers. The currently supported SmartStream
types are `filter`s, which may describe records to keep in or discard
from a stream.

## Writing SmartStreams

See the `examples` directory for full examples.

### Filtering

Create a new cargo project and add `fluvio-smartstream` as a dependency,
along with the following `Cargo.toml` changes.

```toml
[package]
name = "fluvio-wasm-filter"
version = "0.1.0"
authors = ["Fluvio Contributors <team@fluvio.io>"]
edition = "2018"

[lib]
crate-type = ['cdylib']

[dependencies]
fluvio-smartstream = { path = "../../" }
```

Then, write your smartstream using `#[smartstream(filter)]` on your
top-level function. Consider this the "main" function of your SmartStream.

```rust
use fluvio_smartstream::{smartstream, SimpleRecord};

#[smartstream(filter)]
pub fn my_filter(record: &SimpleRecord) -> bool {
    let value = String::from_utf8_lossy(record.value.as_ref());
    value.contains('z')
}
```

This filter will keep only records whose contents contain the letter `z`.

## License

This project is licensed under the [Apache license](LICENSE-APACHE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Fluvio by you, shall be licensed as Apache, without any additional
terms or conditions.
