#!/usr/bin/env bash

set -e

readonly CLI_VERSION=${1-stable}
readonly CLUSTER_VERSION=${2-latest}
readonly TEST_TOPIC=$CLI_VERSION-x-$CLUSTER_VERSION
readonly FLUVIO_BIN=~/.fluvio/bin/fluvio
readonly PAYLOAD_SIZE=1000

function setup_cluster() {
    echo "Installing cluster @ VERSION: $CLUSTER_VERSION"
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=$CLUSTER_VERSION bash
    echo "Starting cluster"

    if [ $CLUSTER_VERSION = "latest" ]; then
        $FLUVIO_BIN cluster start --image-version latest 
    else
        $FLUVIO_BIN cluster start
    fi
}

function setup_cli() {
    echo "Installing CLI @ VERSION: $CLI_VERSION"
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=$CLI_VERSION bash
    $FLUVIO_BIN version
}

function run_test() {
    local RANDOM_DATA=$(tr -cd '[:alnum:]' < /dev/urandom | fold -w${PAYLOAD_SIZE} | head -n1)

    $FLUVIO_BIN topic create $TEST_TOPIC
    echo $RANDOM_DATA | $FLUVIO_BIN produce $TEST_TOPIC
    $FLUVIO_BIN consume $TEST_TOPIC -B -d
    # TODO: Verify the test output matches
}

function cleanup() {
    echo "Deleting cluster"
    $FLUVIO_BIN cluster delete
}

function main() {

    # Run initial cleanup if we're not in CI.
    if [[ -z "$CI" ]];
    then
        cleanup;
    fi

    setup_cluster $CLUSTER_VERSION;
    setup_cli $CLI_VERSION;
    run_test;
    cleanup;
}

main;