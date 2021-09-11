#!/usr/bin/env bash

PUBLISH_CRATES=(
    fluvio-protocol-core
    fluvio-smartstream-derive
    fluvio-types
    fluvio-protocol-derive
    fluvio-protocol-codec
    fluvio-protocol
    fluvio-dataplane-protocol
    fluvio-socket
    fluvio-stream-model
    fluvio-controlplane-metadata
    fluvio-spu-schema
    fluvio-sc-schema
    fluvio-smartstream
    fluvio
    fluvio-stream-dispatcher
    fluvio-package-index
    fluvio-extension-common
)

for crate in ${PUBLISH_CRATES[@]} ; do
    echo $crate;
    pushd crates/$crate;
    cargo publish
    popd
done