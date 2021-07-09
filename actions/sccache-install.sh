#!/bin/bash
# set up sccache
set -e
MATRIX_OS=${1}
LINK=https://github.com/mozilla/sccache/releases/download
SCCACHE_VERSION=v0.2.15
echo "shell matrix.os=$MATRIX_OS"

if [[ "$MATRIX_OS" == "ubuntu-latest" ]]; then
    echo "Installing SCCACHE"
    SCCACHE_DIR=/home/runner/.cache/sccache
    echo "SCCACHE_DIR=$SCCACHE_DIR" | tee -a $GITHUB_ENV
    SCCACHE_FILE=sccache-$SCCACHE_VERSION-x86_64-unknown-linux-musl
    mkdir -p $HOME/.local/bin
    curl -L "$LINK/$SCCACHE_VERSION/$SCCACHE_FILE.tar.gz" | tar xz
    chmod +x $SCCACHE_FILE/sccache
    mv -f $SCCACHE_FILE/sccache $HOME/.local/bin/sccache
    echo "$HOME/.local/bin" >> $GITHUB_PATH
fi


          