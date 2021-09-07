#!/bin/bash
# set up sccache
set -e
MATRIX_OS=${1}
echo "installing zig matrix.os=$MATRIX_OS"

if [[ "$MATRIX_OS" == "ubuntu-latest" ]]; then
    echo "installing zig on ubuntu"
    sudo snap install --beta --classic zig && \
    sudo apt-get install lld-11 && \
    echo "FLUVIO_BUILD_LLD=lld-11" | tee -a $GITHUB_ENV
fi

if [[ "$MATRIX_OS" == "macos-latest" ]]; then
    echo "installing zig on mac"
 #   brew update
    brew install zig && \
    brew install llvm@11 && \
    echo "FLUVIO_BUILD_LLD=/usr/local/opt/llvm@11/bin/lld" | tee -a $GITHUB_ENV
fi


          