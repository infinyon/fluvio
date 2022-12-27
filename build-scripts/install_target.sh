#!/usr/bin/env bash
set -e

if [ "$TARGET" = "armv7-unknown-linux-gnueabihf" ] || [ "$TARGET" = "arm-unknown-linux-gnueabihf" ]; then
    # Install cross if not installed
    [[ -x "$(command -v cross)" ]] || cargo install cross --locked
    # must perform helm pkg since we dont have access to pkg
    make helm_pkg
fi

if [ -n "$TARGET" ]; then
    echo "loading target $TARGET"
    rustup target add $TARGET
fi
