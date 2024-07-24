#!/usr/bin/env bash
set -e

if [ "$TARGET" = "armv7-unknown-linux-gnueabihf" ] || [ "$TARGET" = "arm-unknown-linux-gnueabihf" ]; then
    cargo install --locked bindgen-cli
fi

if [ -n "$TARGET" ]; then
    echo "loading target $TARGET"
    rustup target add $TARGET
fi
