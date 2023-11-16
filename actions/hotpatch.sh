#!/bin/sh

export RELEASE=true
export REPO_VERSION=0.11.0

make curl-install-fluvio
make install-fluvio-package

GH_RELEASE_TAG=v0.11.0 \
make download-fluvio-release

make publish-artifacts-stable

make bump-fluvio-stable
