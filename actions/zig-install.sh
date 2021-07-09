#!/bin/bash
# set up sccache
set -e
MATRIX_OS=${1}
echo "installing zig matrix.os=$MATRIX_OS"

if [[ "$MATRIX_OS" == "ubuntu-latest" ]]; then
    sudo snap install --beta --classic zig && \
    sudo apt-get install lld-11 && \
    echo "FLUVIO_BUILD_LLD=lld-11" | tee -a $GITHUB_ENV
fi


          