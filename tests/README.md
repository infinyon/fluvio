# Fluvio Test Runner

Used to run a various test against `Fluvio` platform.

This assumes you have read Fluvio doc and setup or access to a Fluvio cluster .



## Setting up Test runer

Build all `Fluvio` crates. 

```
cargo build
```

Set up alias to run development version of Fluvio and Test runner CLI.

```
alias flvd=./target/debug/fluvio
alias flvt=./target/debug/flv-test

```


# Running Test runner

Test runner can be a running in two ways:
- Create new cluster (local or k8) and run tests (smoke test)
- Run tests againts existing cluster


## Smoke test

This run a simple smoke test by creating new local cluster.

It creates a simple topic: `topic0` and perform produce/consume 

```
$ flvt --local

....various cluster startup
start testing...
found topic: topic0 offset: 0
starting produce
Ok!
send message of len 108
topic: topic0, consume message validated!
```

## Test with multiple iteration

Smoke test can be specified with more than 1 iterations:

Run a test with sending 10 records:

```
flvt  --local --produce-iteration 10
```

## Run test without re-installing

After initial test, more iteration can be tested without re-installing cluster

```
 flvt -d --produce-iteration 200
```

## No streaming

By default, produce and consumer run in paralle, but you can force consumer to wait for all producer iteration

```
flvt -d --produce-iteration 400   --consumer-wait
```