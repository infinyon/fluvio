# Fluvio Test Runner

Used to run a various test against `Fluvio` platform.

This assumes you have read Fluvio doc and setup or access to a Fluvio cluster .

## Setting up Test runer

* Build all `Fluvio` crates
* Build development kubernetes image

Must run test runner from fluvio repo root (issue: #[860](https://github.com/infinyon/fluvio/issues/860))

```
make build-test
make build_k8_image 
```

## Testing Fluvio with different cargo compiler profiles

(More about [cargo profiles](https://doc.rust-lang.org/cargo/reference/profiles.html))

### Development profile
By default we test with debug compiler symbols with the `development` profile.

Optional: Set up alias to run development version of Fluvio and Test runner CLI.

```
cargo build
alias fluvio-test=./target/debug/fluvio-test
```

### Release profile

It's important to test Fluvio's release artifact (without the debug symbols) in order to reproduce the same behavior as in production.

This is as easy as adding `--release` to the `cargo build` command.

Optional: Set up alias to run release version of Fluvio and Test runner CLI.

```
cargo build --release
alias fluvio-test=./target/release/fluvio-test
```

# Running Test runner

```
fluvio-test <test-name> [FLAGS] [OPTIONS] -- [SUBCOMMAND OPTIONS]
```

> Test runner testing doesn't work when VERSION set to an unpublished version. Workaround: Run `make build_k8_image` and use `--develop` flag with `fluvio-test` (issue #[859](https://github.com/infinyon/fluvio/issues/859))


Test runner can be a running in two ways:
- Create new cluster (local or k8) and run test
- Run tests against existing cluster

> Expected behavior of local clusters that fluvio-test start:
>
> If you `ctrl+c`, the local cluster will also be terminated.
>
> If you want to deploy a cluster before or delete your cluster after a test, then pass `--cluster-start` or `--cluster-delete` flags, respectively. 
>
> The `--cluster-start-fresh` flag will delete, then re-start a cluster before the test begins

---
## Smoke test

The smoke test can be configured with it's own CLI options:

`fluvio-test smoke [environment vars] -- [smoke test vars]`

---

This is an example of running a simple test, the smoke test. creating new local cluster

It creates a simple topic: `topic` and perform produce/consume 

```
$ fluvio-test smoke --cluster-start --cluster-delete
Start running fluvio test runner
Starting cluster and testing connection
remove cluster skipped
installing cluster
🛠️  Installing Fluvio
     ✅ Fluvio app chart has been installed
🔎 Found SC service addr: 172.18.0.2:32309
👤 Profile set
🤖 SPU group launched (1)
     ✅ All SPUs confirmed
🎯 Successfully installed Fluvio!
Creating the topic: test
topic "test" created
found topic: test offset: 0
starting fetch stream for: test base offset: 0, expected new records: 1
found topic: test offset: 0
total records sent: 0 chunk time: 0.0 secs
consume message validated!, records: 1
replication status verified
performing 2nd fetch check. waiting 5 seconds
performing complete  fetch stream for: test base offset: 0, expected new records: 1
full <<consume test done for: test >>>>
deleting cluster
+--------------+--------------+
| Test Results |              |
+--------------+--------------+
| Pass?        | true         |
+--------------+--------------+
| Duration     | 5.028603585s |
+--------------+--------------+
```

### Test with multiple iteration

Smoke test can be specified with more than 1 iterations:

Run a test with sending 10 records:

```
$ fluvio-test smoke --local -- --producer-iteration 10
```

### Run test using your current Fluvio cluster

```
$ fluvio-test smoke --local -- --producer-iteration 10
$ fluvio-test smoke -- --producer-iteration 200
```

### No streaming

By default, produce and consumer run in parallel, but you can force consumer to wait for all producer iteration

```
$ fluvio-test smoke -- --producer-iteration=400 --consumer-wait
```

## Anatomy of a new test

There are 2 parts to adding new tests.
1. Creating a new test in the `tests` module + annotating with `#[fluvio_test]`
2. Add `#[derive(MyTestCase)]` to test struct (for CLI options) 

### Passing vars from the CLI to your test

* Your test needs to create a struct to hold test-specific variables. (This is required, even if it is empty.) The struct should derive `Parser`.
* The test runner uses this struct in order to parse cli flags for your test with [Parser::parse_from()](https://docs.rs/clap/latest/clap/trait.Parser.html#method.parse_from)

### Test stub of a new test

Write new tests in the `tests` module of the `fluvio-test` crate. 

Here's a complete stub to modify:
```rust
use std::env;

use clap::Parser;
use fluvio_test_derive::fluvio_test;
use fluvio_test_case_derive::MyTestCase;

#[derive(Debug, Clone, Parser, Default, PartialEq, MyTestCase)]
#[clap(name = "Fluvio Test Example Test")]
pub struct ExampleOption {
    #[clap(long)]
    pub verbose: bool,
}

#[fluvio_test()]
pub fn example(mut test_driver: FluvioTestDriver, mut test_case: TestCase) {
    let example_test_case: MyTestCase = test_case.into();

    [...]
}

```

> `TestCase` has an internal field `option` that is `Box<dyn TestOption>`. You'll want to implement `From<TestCase>` on your own struct so you can downcast and use this struct more flexibly.
> 
> See `tests/smoke/mod.rs` or `tests/concurrent/mod.rs` for more example boilerplate code

### Using the fluvio test macro
The `#[fluvio_test]` macro will expand your test to create a topic and check if the `TestCase` meets your test requirements.

Test requirements can be defined by using the following variables with the macro:
* `min_spu` -- default value is `1` if unset

Setting a default topic name can be defined by setting the following with the macro:
* `topic` -- default value: `topic` if unset

example: Set both `min_spu` and `topic` on test.

```rust
#[fluvio_test(min_spu=2, topic="test_topic")]
pub fn run(client: Arc<Fluvio>, mut option: TestCase) {
    println!("Hello world!")
}
```

(For `async/await` test code, use the `async` attribute with the macro)

```rust
#[fluvio_test(min_spu=2, topic="test_topic", async)]
pub async fn run(client: Arc<Fluvio>, mut option: TestCase) {
    println!("Hello world!")
}
```

If this test's name was `example`, we can start this test by running:

`cargo run --bin fluvio-test -- example`

Which would be skipped, because the test requires 2 spu, and by default we only have one.

But if we run it like:

`cargo run --bin fluvio-test -- example --spu 2`

The test would start and a test topic named `test_topic` would be created because the minimum requirements were met.