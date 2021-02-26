#!/usr/bin/env bash

set -e
RELEASE=$1

cargo build $RELEASE --bin fluvio
cargo build $RELEASE --bin produce
cargo build $RELEASE --bin produce-key-value
cargo build $RELEASE --bin consume
cargo build $RELEASE --bin echo
