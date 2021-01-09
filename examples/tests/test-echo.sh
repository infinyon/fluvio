#!/usr/bin/env bash

set -e

cargo run --bin fluvio -- topic delete echo >/dev/null 2>&1 || true
cargo run --bin fluvio -- topic create echo

cargo run --bin echo
