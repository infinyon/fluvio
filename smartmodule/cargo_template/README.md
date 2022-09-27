# Fluvio SmartModules

This repository is a [`cargo-generate`] template for getting started
with writing Fluvio SmartModules. To use it, run the following:

```
$ cargo install cargo-generate
$ cargo generate --git https://github.com/infinyon/fluvio-smartmodule-template
```

> **Note**: To compile a SmartModule, you will need to install the `wasm32-unknown-unknown`
> target by running `rustup target add wasm32-unknown-unknown`.

## About SmartModules

Fluvio SmartModules are custom plugins that can be used to manipulate
streaming data in a topic. SmartModules are written in Rust and compiled
to WebAssembly. To use a SmartModule, you simply provide the `.wasm` file
to the Fluvio consumer, which uploads it to the Streaming Processing Unit
(SPU) where it runs your SmartModule code on each record before sending
it to the consumer.

Below are the various types of SmartModules and examples of how to use them.

### Filtering

Filters are functions that are given a reference to each record in the
stream as it is processed, and must return true or false to determine
whether the record should be kept (true) or discarded (false).

```rust
use fluvio_smartmodule::{smartmodule, SimpleRecord};

#[smartmodule(filter)]
pub fn my_filter(record: &SimpleRecord) -> bool {
    let value = String::from_utf8_lossy(record.value.as_ref());
    value.contains('z')
}
```

This filter will keep only records whose data contains the letter `z`.

## Using SmartModules with the Fluvio CLI

Make sure to follow the [Fluvio getting started] guide, then create a new
topic to send data to.

[Fluvio getting started]: https://www.fluvio.io/docs/getting-started/

```bash
$ fluvio topic create smartmodule-test
$ cargo build --release
$ fluvio consume smartmodule-test -B --{{smartmodule-type}}="target/wasm32-unknown-unknown/release/{{project-name}}"
```
