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
flv-test <test-name> [FLAGS] [OPTIONS] -- [SUBCOMMAND OPTIONS]
```

> Test runner testing doesn't work when VERSION set to an unpublished version. Workaround: Run `make minikube_image` and use `--develop` flag with `flv-test` (issue #[859](https://github.com/infinyon/fluvio/issues/859))


Test runner can be a running in two ways:
- Create new cluster (local or k8) and run test
- Run tests againts existing cluster


> Expected behavior of local clusters that flv-test start:
>
> If you `ctrl+c`, the local cluster will also be terminated.
>
> If you want the cluster to stick around, then you should start the cluster locally, and pass `--disable-install` and `--keep-cluster` flags.

## Benchmark testing
(The feature is experimental.)

* Tests must opt-in to be run in benchmark mode. To opt-in add `#[fluvio_test(benchmark = true)]` to test
* To run a test in benchmark mode, run test with the `--benchmark` flag.

An error message will appear when attempting to benchmark tests without `benchmark = true`.

Benchmarks are performed with the [bencher](https://crates.io/crates/bencher) crate using the [auto_bench](https://docs.rs/bencher/0.1.5/bencher/struct.Bencher.html#method.auto_bench) method.

The number of iterations are not user-controllable because it is the easiest way to use the crate to build a statistical summary.

Total iterations run depend on the runtime of a single test. The longer the test, the fewer the runs.

---
## Smoke test

The smoke test can be configured with it's own CLI options:

`flv-test smoke [environment vars] -- [smoke test vars]`


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
$ flv-test smoke --local -- --producer-iteration 10
```

### Run test without re-installing

After initial test using `--keep-cluster`, more iteration can be tested without re-installing cluster using `--disable-install`

```
$ flv-test smoke --local --keep-cluster -- --producer-iteration 10
$ flv-test smoke --disable-install -- --producer-iteration 200
```

### No streaming

By default, produce and consumer run in parallel, but you can force consumer to wait for all producer iteration

```
$ flv-test smoke -- --producer-iteration=400 --consumer-wait
```

## Anatomy of a new test

There are 4 parts to adding new tests.
1. Implementing `TestOptions` for test specific CLI arguments
    - Naming convention (in pascal case): `<testname>TestOption`
2. Implmenenting `From<TestCase>` for your test case struct to downcast to.
    - Naming convention (in pascal case): `<testname>TestCase`
3. Creating a new test in the `tests` module + annotating with `#[fluvio_test]`

### Passing vars from the CLI to your test

* Your test needs to create a struct to hold test-specific variables. (This is required, even if it is empty.) The struct should derive `StructOpt` and implement the `TestOptions` trait.
* The test runner uses this struct in order to parse cli flags for your test with [StructOpt::from_iter()](https://docs.rs/structopt/0.3.21/structopt/trait.StructOpt.html#method.from_iter)

### Test stub of a new test

Write new tests in the `tests` module of the `flv-test` crate. 

Here's a complete stub to modify:
```rust
use std::sync::Arc;
use std::any::Any;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_future::task::spawn;
use fluvio_integration_derive::fluvio_test;
use fluvio_test_util::test_meta::environment::EnvironmentSetup;
use fluvio_test_util::test_meta::{TestOption, TestCase, TestResult};

#[derive(Debug, Clone)]
pub struct ExampleTestCase {
    pub environment: EnvironmentSetup,
    pub option: ExampleTestOption,
}

impl From<TestCase> for ExampleTestCase {
    fn from(test_case: TestCase) -> Self {
        let example_option = test_case
            .option
            .as_any()
            .downcast_ref::<ExampleTestOption>()
            .expect("ExampleTestOption")
            .to_owned();
        ExampleTestCase {
            environment: test_case.environment,
            option: example_option,
        }
    }
}

// For CLI options
#[derive(Debug, Clone, StructOpt, Default, PartialEq)]
#[structopt(name = "Fluvio Example Test")]
pub struct ExampleTestOption {}

impl TestOption for ExampleTestOption {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[fluvio_test()]
pub async fn run(client: Arc<Fluvio>, mut option: TestCase) {
    let example_test_case : ExampleTestCase = option.into();

    println!("Ready to run tests: {:?}", example_test_case);
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
pub async fn run(client: Arc<Fluvio>, mut option: TestCase) {
    println!("Hello world!")
}
```

If this test's name was `example`, we can start this test by running:

`cargo run --bin flv-test -- example`

Which would be skipped, because the test requires 2 spu, and by default we only have one.

But if we run it like:

`cargo run --bin flv-test -- example --spu 2`

The test would start and a test topic named `test_topic` would be created because the minimum requirements were met.