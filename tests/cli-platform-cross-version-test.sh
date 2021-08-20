#!/usr/bin/env bash

set -e

readonly CLI_VERSION=${1-stable}
readonly CLUSTER_VERSION=${2-latest}
readonly TEST_TOPIC=$CLI_VERSION-x-$CLUSTER_VERSION
readonly FLUVIO_BIN=~/.fluvio/bin/fluvio
readonly PAYLOAD_SIZE=${PAYLOAD_SIZE:-100}
readonly CI_SLEEP=${CI_SLEEP:-5}
readonly CI=${CI:-}

# If we're in CI, we want to slow down execution
# to give CPU some time to rest, so we don't time out
function ci_check() {
    if [[ ! -z "$CI" ]];
    then
        echo "[CI MODE] Pausing for ${CI_SLEEP} second(s)";
        w | head -1
        sleep ${CI_SLEEP};
    fi
}

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
    ci_check;

    $FLUVIO_BIN topic create $TEST_TOPIC
    ci_check;

    echo $RANDOM_DATA | $FLUVIO_BIN produce $TEST_TOPIC
    ci_check;

    $FLUVIO_BIN consume $TEST_TOPIC -B -d
    ci_check;
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

    setup_cluster $CLUSTER_VERSION;
    ci_check;
    setup_cli $CLI_VERSION;
    ci_check;
    run_test;
    cleanup;
}

main;