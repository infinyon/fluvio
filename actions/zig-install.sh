#!/bin/bash
# Install Zig
# This is optimized for installing on GitHub Actions
set -e
MATRIX_OS=${1}
ZIG_VER=0.12.1
ARCH=x86_64
OS=linux


# auto detect OS
unameOS="$(uname -s)"
case "${unameOS}" in
    Linux*)     OS=linux;;
    Darwin*)    OS=macos;;
    *)          machine="UNKNOWN:${unameOut}"
esac

unameARCH="$(uname -m)"
case "${unameARCH}" in
    arm64*)     ARCH=aarch64;;
    aarch64*)   ARCH=aarch64;;
    x86_64*)    ARCH=x86_64;;
    *)          machine="UNKNOWN:${unameARCH}"
esac

echo "installing zig $ZIG_VER on $OS $ARCH"

wget https://ziglang.org/download/$ZIG_VER/zig-$OS-$ARCH-$ZIG_VER.tar.xz && \
tar -xf zig-$OS-$ARCH-$ZIG_VER.tar.xz && \
sudo mv zig-$OS-$ARCH-$ZIG_VER /usr/local && \
pushd /usr/local/bin && \
sudo ln -s ../zig-$OS-$ARCH-$ZIG_VER/zig . && \
popd && \
sudo rm zig-$OS-$ARCH-$ZIG_VER.tar.*
