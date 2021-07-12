#!/bin/bash
# set up sccache
set -e
MATRIX_OS=${1}

echo "Starting SCCACHE"
sccache --stop-server || true


          