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

---

Test runner can be a running in two ways:
- Create new cluster (local or k8) and run test
- Run tests againts existing cluster


## Smoke test

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
Executing: /home/telant/Documents/fluvio/target/debug/fluvio produce topic
Ok!
send message of len 121
Executing> /home/telant/Documents/fluvio/target/debug/fluvio consume topic --partition 0 -d -o 0
topic: topic, consume message validated!
deleting cluster
```

## Test with multiple iteration

Smoke test can be specified with more than 1 iterations:

Run a test with sending 10 records:

```
$ flv-test smoke --local --produce-iteration 10
```

## Run test without re-installing

After initial test using `--skip-cluster-delete`, more iteration can be tested without re-installing cluster using `--skip-cluster-start`

```
$ flv-test smoke --local --produce-iteration 10 --skip-cluster-delete
$ flv-test smoke --skip-cluster-start --produce-iteration 200
```

## No streaming

By default, produce and consumer run in parallel, but you can force consumer to wait for all producer iteration

```
$ flv-test smoke --produce-iteration 400 --consumer-wait
```