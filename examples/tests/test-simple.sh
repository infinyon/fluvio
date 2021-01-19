#!/usr/bin/env bash

set -e
RELEASE=$1

# Tests the 00-produce and 01-consume examples

cargo run $RELEASE --bin fluvio -- topic delete simple || true
cargo run $RELEASE --bin fluvio -- topic create simple

cargo build $RELEASE --bin produce
cargo build $RELEASE --bin consume

produce_stdout=$(cargo run $RELEASE --bin produce & sleep 1 && exit)
consume_stdout=$(cargo run $RELEASE --bin consume & sleep 1 && exit)

# Assert the output of consume and produce are identical
diff <(echo "$produce_stdout") <(echo "$consume_stdout")

# Assert that the output is "Hello, Fluvio!"
[[ "${produce_stdout}" == "Hello, Fluvio!" ]] || {
  echo "Expected output to be 'Hello, Fluvio!'"
  exit 1
}
