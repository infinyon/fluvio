#!/usr/bin/env bash

set -e

# Tests the 00-produce and 01-consume examples

cargo run --bin fluvio -- topic delete simple-example >/dev/null 2>&1 || true
cargo run --bin fluvio -- topic create simple-example

cargo build --bin produce
cargo build --bin consume

produce_stdout=$(cargo run --bin produce & sleep 1 && exit)
consume_stdout=$(cargo run --bin consume & sleep 1 && exit)

# Assert the output of consume and produce are identical
diff <(echo "$produce_stdout") <(echo "$consume_stdout")

# Assert that the output is "Hello, Fluvio!"
[[ "${produce_stdout}" == "Hello, Fluvio!" ]] || {
  echo "Expected output to be 'Hello, Fluvio!'"
  exit 1
}
