#!/bin/bash
# set up sccache
set -e
MATRIX_OS=${1}
ZIG_VER=0.9.1
echo "installing zig matrix.os=$MATRIX_OS"

if [[ "$MATRIX_OS" == "ubuntu-latest" ]]; then
    echo "installing zig on ubuntu"
    cd /tmp
    wget https://ziglang.org/download/$ZIG_VER/zig-linux-x86_64-$ZIG_VER.tar.xz
    sudo mv zig-linux-x86_64-$ZIG_VER /usr/local
    cd /usr/local/bin
    sudo ln -s ../zig-linux-x86_64-$ZIG_VER/zig .
    sudo ${0%/*}/llvm.sh 13 && \
    echo "FLUVIO_BUILD_LLD=lld-13" | tee -a $GITHUB_ENV
fi

if [[ "$MATRIX_OS" == "macos-11" ]]; then
    echo "installing zig on mac"
 #   brew update
    brew install zig && \
    echo "FLUVIO_BUILD_LLD=/opt/homebrew/opt/llvm@13/bin/lld" | tee -a $GITHUB_ENV
fi


        