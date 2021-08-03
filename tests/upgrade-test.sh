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
# ./upgrade-test.sh [current stable version] [dev version]
#
# If CI env var is set, we will upgrade to VERSION=latest from public installer
# If DEBUG env var is set, the bash session will be extra verbose
# If USE_LATEST is set, cluster will upgrade as if in CI mode, but without the pausing

set -e


readonly STABLE=${1:-stable}
readonly PRERELEASE=${2:-$(cat VERSION)-$(git rev-parse HEAD)}
readonly CI_SLEEP=${CI_SLEEP:-5}
readonly CI=${CI:-}
readonly STABLE_TOPIC=${STABLE_TOPIC:-stable}
readonly PRERELEASE_TOPIC=${PRERELEASE_TOPIC:-prerelease}
readonly USE_LATEST=${USE_LATEST:-}
readonly FLUVIO_BIN=$(readlink -f ${FLUVIO_BIN:-"$(which fluvio)"})

# Change to this script's directory 
pushd "$(dirname "$(readlink -f "$0")")" > /dev/null

function cleanup() {
    echo Clean up test data
    rm -f --verbose ./*.txt.tmp;
    rm -f --verbose ./*.checksum;
    echo Delete cluster if possible
    $FLUVIO_BIN cluster delete || true
    $FLUVIO_BIN cluster delete --sys || true

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

    echo "Install (current stable) CLI"
    unset VERSION
    curl -fsS https://packages.fluvio.io/v1/install.sh | bash

    local STABLE_FLUVIO=${HOME}/.fluvio/bin/fluvio

    echo "Installing stable fluvio"
    $STABLE_FLUVIO cluster start 
    ci_check;

    $STABLE_FLUVIO version
    ci_check;


    echo "Create test topic: ${STABLE_TOPIC}"
    $STABLE_FLUVIO topic create ${STABLE_TOPIC} 
    # $STABLE_FLUVIO topic create ${STABLE_TOPIC}-delete 
    ci_check;

    cat data1.txt.tmp | $STABLE_FLUVIO produce ${STABLE_TOPIC}
    # cat data1.txt.tmp | $STABLE_FLUVIO produce ${STABLE_TOPIC}-delete
    ci_check;

    echo "Validate test data w/ v${STABLE} CLI matches expected data created BEFORE upgrading cluster + CLI to v${PRERELEASE}"
    $STABLE_FLUVIO consume -B -d ${STABLE_TOPIC} 2>/dev/null | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c stable-cli-stable-topic.checksum; then
        echo "${STABLE_TOPIC} topic validated with v${STABLE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat stable-cli-stable-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    # echo "Validate deleting topic created by v${STABLE} CLI"
    # $STABLE_FLUVIO topic delete ${STABLE_TOPIC}-delete 
    echo "stable validated"


    $STABLE_FLUVIO partition list

}

# This function is intended to be run last after the Stable validation
# We install the Prerelease CLI (either the dev prerelease, or compiled if we're in CI), and upgrade the existing cluster
# Another brand new topic is created, and we do a produce + consume and validate the checksum of the output on that topic
# Then we produce + consume on the Stable + Stable-1 topic and validate the checksums on each of those topics
function validate_upgrade_cluster_to_prerelease() {

    local FLUVIO_BIN_ABS_PATH=$(readlink -f $FLUVIO_BIN)
    local TARGET_VERSION=${PRERELEASE}

    # Change dir to get access to Helm charts
    pushd ..
    if [[ ! -z "$USE_LATEST" ]];
    then
        echo "Download the latest published dev CLI"
        TARGET_VERSION=$(curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=latest bash | grep "Downloading Fluvio" | awk '{print $5}' | sed 's/[+]/-/')
        echo "Installed CLI version ${TARGET_VERSION}"
        FLUVIO_BIN_ABS_PATH=${HOME}/.fluvio/bin/fluvio
        echo "Upgrading cluster to ${TARGET_VERSION}"
        $FLUVIO_BIN_ABS_PATH cluster upgrade --sys
        $FLUVIO_BIN_ABS_PATH cluster upgrade --image-version latest
        echo "Wait for SPU to be upgraded. sleeping 1 minute"
        sleep 60
    else
        echo "Test local image v${PRERELEASE}"
        # This should use the binary that the Makefile set

        echo "Using Fluvio binary located @ ${FLUVIO_BIN_ABS_PATH}"
        $FLUVIO_BIN_ABS_PATH cluster upgrade --sys
        $FLUVIO_BIN_ABS_PATH cluster upgrade --develop

        helm list
        kubectl get configmap spu-k8 -o yaml
        kubectl get pods 
        kubectl get pod -l app=fluvio-sc -o yaml
        echo "Wait for SPU to be upgraded. sleeping 1 minute"
        sleep 60
    fi
    popd

    ci_check;

    $FLUVIO_BIN_ABS_PATH version
    ci_check;

    echo "Create test topic: ${PRERELEASE_TOPIC}"
    $FLUVIO_BIN_ABS_PATH topic create ${PRERELEASE_TOPIC}
    ci_check;

    cat data2.txt.tmp | $FLUVIO_BIN_ABS_PATH produce ${PRERELEASE_TOPIC}
    ci_check;

    echo "Validate test data w/ v${TARGET_VERSION} CLI matches expected data AFTER upgrading cluster + CLI to v${TARGET_VERSION}"
    $FLUVIO_BIN_ABS_PATH consume -B -d ${PRERELEASE_TOPIC} 2>/dev/null | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c prerelease-cli-prerelease-topic.checksum; then
        echo "${PRERELEASE_TOPIC} topic validated with v${TARGET_VERSION} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat prerelease-cli-prerelease-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    # Exercise older topics
    cat data2.txt.tmp | $FLUVIO_BIN_ABS_PATH produce ${STABLE_TOPIC}
    ci_check;

    echo "Validate v${STABLE} test data w/ ${TARGET_VERSION} CLI matches expected data AFTER upgrading cluster + CLI to v${TARGET_VERSION}"
    $FLUVIO_BIN_ABS_PATH consume -B -d ${STABLE_TOPIC} | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c prerelease-cli-stable-topic.checksum; then
        echo "${STABLE_TOPIC} topic validated with v${TARGET_VERSION} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat prerelease-cli-stable-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    #echo "Validate deleting topic created by v${STABLE} CLI"
    # $FLUVIO_BIN_ABS_PATH topic delete ${STABLE_TOPIC} 

    # echo "Validate deleting topic created by v${TARGET_VERSION} CLI"
    #$FLUVIO_BIN_ABS_PATH topic delete ${PRERELEASE_TOPIC} 
}

# Create 2 base data files and calculate checksums for the expected states of each of our testing topics
# Build produce/consume with generated data to validate integrity across upgrades
function create_test_data() {
    local TEST_DATA_BYTES=${TEST_DATA_BYTES:-100}

    # The baseline files
    for BASE in {1..2}
    do
        echo "Create the baseline file #${BASE}"
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