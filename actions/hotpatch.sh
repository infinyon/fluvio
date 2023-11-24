#!/bin/sh

export RELEASE=true
export REPO_VERSION=0.11.1

make curl-install-fluvio
make install-fluvio-package

GH_RELEASE_TAG=v0.11.1 \
make download-fluvio-release

make publish-artifacts-stable

make bump-fluvio-stable