#!/usr/bin/env bash

set -e

cargo run --bin fluvio -- topic delete echo || true
cargo run --bin fluvio -- topic create echo

cargo run --bin echo
