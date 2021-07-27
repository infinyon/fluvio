#!/usr/bin/env bash

# The strategy for this test is to write to the cluster and roll forward
# with release upgrades
# We track the previous release, current release and the prerelease
#
# We create new topic per round.
# We'll produce and consume from all test topics per round.
# Content is verified via checksum
#
# Usage:
# ./upgrade-test.sh <current stable version> [dev version]
#
# If CI env var is set, we will build fluvio code and upgrade to local develop image
# If DEBUG env var is set, the bash session will be extra verbose

set -e

# Change to this script's directory 
pushd "$(dirname "$(readlink -f "$0")")" > /dev/null

readonly STABLE=${1:-stable}
readonly PRERELEASE=${2:-$(cat ../VERSION)-$(git rev-parse HEAD)}
readonly CI_SLEEP=${CI_SLEEP:-10}
readonly CI=${CI:-}
readonly STABLE_TOPIC=${STABLE_TOPIC:-stable-cli-topic}
readonly PRERELEASE_TOPIC=${PRERELEASE_TOPIC:-prerelease-cli-topic}


function cleanup() {
    echo Clean up test data
    rm -f --verbose ./*.txt.tmp;
    rm -f --verbose ./*.checksum;
    echo Delete cluster if possible
    fluvio cluster delete || true
}

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

# This function is intended to be run second after the Stable-1 validation
# We install the Stable CLI, and upgrade the existing cluster
# A brand new topic is created, and we do a produce + consume and validate the checksum of the output on that topic
# Then we produce + consume on the Stable-1 topic and validate the checksums on that topic
function validate_cluster_stable() {

    echo "Install (current stable) v${STABLE} CLI"
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=${STABLE} bash

    fluvio cluster start 
    ci_check;

    fluvio version
    ci_check;

    echo "Create test topic: ${STABLE_TOPIC}"
    fluvio topic create ${STABLE_TOPIC} 
    fluvio topic create ${STABLE_TOPIC}-delete 
    ci_check;

    cat data1.txt.tmp | fluvio produce ${STABLE_TOPIC}
    cat data1.txt.tmp | fluvio produce ${STABLE_TOPIC}-delete
    ci_check;

    echo "Validate test data w/ v${STABLE} CLI matches expected data created BEFORE upgrading cluster + CLI to v${PRERELEASE}"
    fluvio consume -B -d ${STABLE_TOPIC} 2>/dev/null | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c stable-cli-stable-topic.checksum; then
        echo "${STABLE_TOPIC} topic validated with v${STABLE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat stable-cli-stable-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    echo "Validate deleting topic created by v${STABLE} CLI"
    fluvio topic delete ${STABLE_TOPIC}-delete 

}

# This function is intended to be run last after the Stable validation
# We install the Prerelease CLI (either the dev prerelease, or compiled if we're in CI), and upgrade the existing cluster
# Another brand new topic is created, and we do a produce + consume and validate the checksum of the output on that topic
# Then we produce + consume on the Stable + Stable-1 topic and validate the checksums on each of those topics
function validate_upgrade_cluster_to_prerelease() {

    if [[ ! -z "$CI" ]];
    then
        echo "[CI MODE] Build and test the dev image v${PRERELEASE}"
        pushd ..

        local FLUVIO_BIN="$(pwd)/fluvio"
        $FLUVIO_BIN cluster upgrade --chart-version=${PRERELEASE} --develop
        popd
    else 
        echo "Build and test the latest published dev image v${PRERELEASE}"
        echo "Install prerelease v${PRERELEASE} CLI"
        curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=latest bash
        local FLUVIO_BIN=`which fluvio`
        $FLUVIO_BIN cluster upgrade --chart-version=${PRERELEASE}
    fi

    ci_check;

    $FLUVIO_BIN version
    ci_check;

    echo "Create test topic: ${PRERELEASE_TOPIC}"
    $FLUVIO_BIN topic create ${PRERELEASE_TOPIC}
    ci_check;

    cat data2.txt.tmp | fluvio produce ${PRERELEASE_TOPIC}
    ci_check;

    echo "Validate test data w/ v${PRERELEASE} CLI matches expected data AFTER upgrading cluster + CLI to v${PRERELEASE}"
    $FLUVIO_BIN consume -B -d ${PRERELEASE_TOPIC} 2>/dev/null | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c prerelease-cli-prerelease-topic.checksum; then
        echo "${PRERELEASE_TOPIC} topic validated with v${PRERELEASE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat prerelease-cli-prerelease-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    # Exercise older topics
    cat data2.txt.tmp | fluvio produce ${STABLE_TOPIC}
    ci_check;

    echo "Validate v${STABLE} test data w/ ${PRERELEASE} CLI matches expected data AFTER upgrading cluster + CLI to v${PRERELEASE}"
    $FLUVIO_BIN consume -B -d ${STABLE_TOPIC} | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c prerelease-cli-stable-topic.checksum; then
        echo "${STABLE_TOPIC} topic validated with v${PRERELEASE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat prerelease-cli-stable-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    echo "Validate deleting topic created by v${STABLE} CLI"
    $FLUVIO_BIN topic delete ${STABLE_TOPIC} 

    echo "Validate deleting topic created by v${PRERELEASE} CLI"
    $FLUVIO_BIN topic delete ${PRERELEASE_TOPIC} 
}

# Create 2 base data files and calculate checksums for the expected states of each of our testing topics
# Build produce/consume with generated data to validate integrity across upgrades
function create_test_data() {
    local TEST_DATA_BYTES=${TEST_DATA_BYTES:-100}

    # The baseline files
    for BASE in {1..2}
    do
        echo "Create the baseline file \#${BASE}"
        local RANDOM_DATA=$(tr -cd '[:alnum:]' < /dev/urandom | fold -w${TEST_DATA_BYTES} | head -n1)
        echo ${RANDOM_DATA} | tee -a data${BASE}.txt.tmp
    done

    # Test stable cli against stable topic 
    echo "Create expected topic checksum for Stable cli x Stable topic"
    cat data1.txt.tmp | shasum | tee -a stable-cli-stable-topic.checksum

    # Test prerelease cli against stable topic
    echo "Create expected topic contents for Prerelease cli x Stable topic"
    cat data1.txt.tmp data2.txt.tmp | tee -a prerelease-cli-stable-topic.txt.tmp
    echo "Create expected topic checksum for Prerelease cli x Stable topic"
    cat prerelease-cli-stable-topic.txt.tmp | shasum | tee -a prerelease-cli-stable-topic.checksum

    # Test preprelease cli against prerelease topic 
    echo "Create expected topic checksum for Prerelease cli x Prerelease topic"
    cat data2.txt.tmp | shasum | tee -a prerelease-cli-prerelease-topic.checksum
}

function main() {

    if [[ ! -z "$DEBUG" ]];
    then
        set -exu
        echo "[DEBUG] command: $0 $@"
    fi

    cleanup;
    create_test_data;

    echo "Update cluster to stable v${STABLE}. Create and validate data."
    validate_cluster_stable;

    echo "Update cluster to prerelease v${PRERELEASE}"
    validate_upgrade_cluster_to_prerelease;

    cleanup;

    # Change back to original directory
    popd > /dev/null
}

main;