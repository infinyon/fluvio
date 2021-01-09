#!/usr/bin/env bash

set -e

fluvio topic delete echo >/dev/null 2>&1 || true
fluvio topic create echo

cargo run --bin echo
