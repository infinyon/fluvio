#!/usr/bin/env bash

set -e
RELEASE=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Run tests
"$DIR/test-simple.sh" "$RELEASE"
"$DIR/test-echo.sh" "$RELEASE"
