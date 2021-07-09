#!/bin/bash
# set up sccache
set -e
MATRIX_OS=${1}

if [[ "$MATRIX_OS" == "ubuntu-latest" ]]; then
    echo "Starting SCCACHE"
    sccache --start-server
fi


          