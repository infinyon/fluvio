#!/usr/bin/env bash

set -e
RELEASE=$1

cargo run $RELEASE --bin fluvio -- topic delete echo || true
cargo run $RELEASE --bin fluvio -- topic create echo

cargo run $RELEASE --bin echo
