#!/usr/bin/env bash
set -e

if [ "$TARGET" = "armv7-unknown-linux-gnueabihf" ] || [ "$TARGET" = "arm-unknown-linux-gnueabihf" ]; then
    # Install cross if not installed
    [[ -x "$(command -v cross)" ]] || cargo install cross
fi

if [ -n "$TARGET" ]; then
    echo "loading target $TARGET"
    rustup target add $TARGET
fi