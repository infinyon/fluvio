# Fluvio Test Runner

Used to run a various test against `Fluvio` platform.

This assumes you have read Fluvio doc and setup or access to a Fluvio cluster .

## Setting up Test runer

* Build all `Fluvio` crates
* Build development minikube image

Must run test runner from fluvio repo root (issue: #[860](https://github.com/infinyon/fluvio/issues/860))

```
make build_test
make minikube-image
```

## Testing Fluvio with different cargo compiler profiles

(More about [cargo profiles](https://doc.rust-lang.org/cargo/reference/profiles.html))

### Development profile
By default we test with debug compiler symbols with the `development` profile.

Optional: Set up alias to run development version of Fluvio and Test runner CLI.

```
cargo build
alias flv-test=./target/debug/flv-test
```

### Release profile

It's important to test Fluvio's release artifact (without the debug symbols) in order to reproduce the same behavior as in production.

This is as easy as adding `--release` to the `cargo build` command.

Optional: Set up alias to run release version of Fluvio and Test runner CLI.

```
cargo build --release
alias flv-test=./target/release/flv-test
```

# Running Test runner

```
flv-test [FLAGS] [OPTIONS] [test-name]
```

> Test runner testing doesn't work when VERSION set to an unpublished version. Workaround: Run `make minikube_image` and use `--develop` flag with `flv-test` (issue #[859](https://github.com/infinyon/fluvio/issues/859))


Test runner can be a running in two ways:
- Create new cluster (local or k8) and run test
- Run tests againts existing cluster

---
## Smoke test

The smoke test can be configured by passing the overriding the following test variables:

(No spaces between key, the `=`, and value)

(e.g. `-v key1=val1 -v key2=val2`)
* `use_cli` (default false)
* `producer.iteration` (default 1)
* `producer.record_size` (default 100)
* `consumer.wait` (default false)

---

This run a simple smoke test by creating new local cluster.

It creates a simple topic: `topic` and perform produce/consume 

```
$ flv-test smoke --local

Start running fluvio test runner
deleting cluster
installing cluster
Performing pre-flight checks
✅ ok: Supported helm version is installed
✅ ok: Supported kubernetes version is installed
✅ ok: Kubernetes config is loadable
✅ ok: Fluvio system charts are installed
Creating the topic: topic
found topic: topic offset: 0
starting produce
Ok!
send message of len 121
topic: topic, consume message validated!
deleting cluster
```

### Test with multiple iteration

Smoke test can be specified with more than 1 iterations:

Run a test with sending 10 records:

```
$ flv-test smoke --local --var producer.iteration=10
```

### Run test without re-installing

After initial test using `--keep-cluster`, more iteration can be tested without re-installing cluster using `--disable-install`

```
$ flv-test smoke --local --var producer.iteration=10 --keep-cluster
$ flv-test smoke --disable-install --var producer.iteration=200
```

### No streaming

By default, produce and consumer run in parallel, but you can force consumer to wait for all producer iteration

```
$ flv-test smoke --var producer-iteration=400 --var consumer.wait
```

## Writing new tests

Create new tests in the `tests` module of the `flv-test` crate. 

Here's a stub to modify:
```rust
use fluvio_integration_derive::fluvio_test;
use fluvio_test_util::test_meta::TestCase;
use fluvio::Fluvio;
use std::sync::Arc;

#[fluvio_test()]
pub async fn run(client: Arc<Fluvio>, option: TestCase) {
    println!("Test stub");
}
```

### Passing vars from the CLI to your test

Custom test vars can be passed in through the CLI using the `--var` or `-v` flags.

You can set multiple variables by passing many `--var`s (e.g. `--var key1=val1 --var key2=val2`)

At the time of this writing, the values are not validated or type checked by the test runner. The test is responsible for checking if these values have been passed in and type checked or whether to fall back to a default value.

example from smoke test:
Check if `producer.iteration` is set, or fall back to value `&"1".to_string()` 

```rust
#[fluvio_test()]
pub async fn run(client: Arc<Fluvio>, option: TestCase) {
    println!(
        "producer.iteration: {}",
        option
            .vars
            .get("producer.iteration")
            .unwrap_or(&"1".to_string())
    );
}
```

### Using the fluvio test macro
The `#[fluvio_test]` macro will expand your test to create a topic and check if the `TestCase` meets your test requirements.

Test requirements can be defined by using the following variables with the macro:
* `min_spu` -- default value is `1` if unset

Setting a default topic name can be defined by setting the following with the macro:
* `topic` -- default value: `topic` if unset

example: Set both `min_spu` and `topic` on test.

```rust
#[fluvio_test(min_spu=2, topic="test_topic")]
pub async fn example(client: Arc<Fluvio>, option: TestCase) {
    println!("Hello world!")
}
```

If this test's name was `example`, we can start this test by running:

`cargo run --bin flv-test -- example`

Which would be skipped, because the test requires 2 spu, and by default we only have one.

But if we run it like:

`cargo run --bin flv-test -- example --spu 2`

The test would start and a test topic named `test_topic` would be created because the minimum requirements were met.