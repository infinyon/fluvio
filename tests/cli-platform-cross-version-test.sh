#!/usr/bin/env bash

set -e

readonly CLI_VERSION=${1-stable}
readonly CLUSTER_VERSION=${2-latest}
readonly TEST_TOPIC=$CLI_VERSION-x-$CLUSTER_VERSION
readonly PAYLOAD_SIZE=${PAYLOAD_SIZE:-100}
readonly CI_SLEEP=${CI_SLEEP:-5}
readonly CI=${CI:-}
readonly SKIP_SETUP=${SKIP_SETUP:-}
readonly SKIP_CLEANUP=${SKIP_CLEANUP:-}

if [[ -z "$SKIP_SETUP" ]];
then
    # This will default to the Makefile value if not provided
    readonly FLUVIO_BIN=${FLUVIO_BIN?Set FLUVIO_BIN if skipping setup}
else
    # We want to avoid letting Makefile set bin path to target dir
    readonly FLUVIO_BIN=~/.fluvio/bin/fluvio
fi

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
    local TEST_DATA=$(shuf -zer -n${PAYLOAD_SIZE}  {A..Z} {a..z} {0..9} | tr -d '\0')

    $FLUVIO_BIN topic create $TEST_TOPIC || true

    echo $TEST_DATA | $FLUVIO_BIN produce $TEST_TOPIC

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
    else
        echo "[CI MODE] Skipping initial cleanup";
    fi

    if [[ -z "$SKIP_SETUP" ]];
    then
        setup_cluster $CLUSTER_VERSION;
        setup_cli $CLI_VERSION;
    else
        echo "Skipping setup"
    fi

    run_test;

    if [[ -z "$SKIP_CLEANUP" ]];
    then
        cleanup;
    else
        echo "Skipping cleanup"
    fi

}

main;