#!/usr/bin/env bash
set -e

if [ -n "$TARGET" ]; then
    echo "loading target $TARGET"
    rustup target add $TARGET
fi