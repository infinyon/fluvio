#!/bin/sh

export RELEASE=true
export REPO_VERSION=0.11.0

GH_RELEASE_TAG=v0.11.0 \
make download-fluvio-release

make publish-artifacts-stable

make bump-fluvio-stable
