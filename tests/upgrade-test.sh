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
# ./upgrade-test.sh <older stable version> <current stable version> [dev version]
#
# If dev version is set, 
# If CI env var is set, we will build fluvio code and upgrade to local develop image


set -exu
#set -e

echo command: $0 $*

readonly STABLE_MINUS_ONE=${1:?Please provide a starting cluster version for arg 1}
readonly STABLE=${2:?Please provide a second cluster version for arg 2}
readonly PRERELEASE=${3:-$(cat ../VERSION)-$(git rev-parse HEAD)}
readonly CI_SLEEP=${CI_SLEEP:-10}
readonly CI=${CI:-}
readonly STABLE_MINUS_ONE_TOPIC=${STABLE_MINUS_ONE_TOPIC:-stable-minus-one-cli-topic}
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

# This function is intended to be run first after generating test data
# We install the Stable-1 CLI, and start a cluster
# A topic is created, and we do a produce + consume and validate the checksum of the output
function validate_cluster_out_of_date_stable() {

    echo "Install (out-of-date) v${STABLE_MINUS_ONE} CLI"
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=${STABLE_MINUS_ONE} bash

    echo "Start v${STABLE_MINUS_ONE} cluster"
    fluvio cluster start
    ci_check;

    fluvio version
    ci_check;

    echo "Create test topic: ${STABLE_MINUS_ONE_TOPIC}"
    fluvio topic create ${STABLE_MINUS_ONE_TOPIC} 
    ci_check;

    cat data1.txt.tmp | fluvio produce ${STABLE_MINUS_ONE_TOPIC}
    ci_check;

    echo "Validate test data w/ v${STABLE_MINUS_ONE} CLI matches expected data created BEFORE upgrading cluster + CLI to ${STABLE}"
    fluvio consume -B -d ${STABLE_MINUS_ONE_TOPIC} | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c stable-minus-one-cli-stable-minus-one-topic.checksum; then
        echo "${STABLE_MINUS_ONE_TOPIC} topic validated with v${STABLE_MINUS_ONE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat stable-minus-one-cli-stable-minus-one-topic.checksum | awk '{print $1}')"
        exit 1
    fi
}


# This function is intended to be run second after the Stable-1 validation
# We install the Stable CLI, and upgrade the existing cluster
# A brand new topic is created, and we do a produce + consume and validate the checksum of the output on that topic
# Then we produce + consume on the Stable-1 topic and validate the checksums on that topic
function validate_upgrade_cluster_to_stable() {

    echo "Install (current stable) v${STABLE} CLI"
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=${STABLE} bash

    fluvio cluster upgrade
    ci_check;

    fluvio version
    ci_check;

    echo "Create test topic: ${STABLE_TOPIC}"
    fluvio topic create ${STABLE_TOPIC} 
    ci_check;

    cat data2.txt.tmp | fluvio produce ${STABLE_TOPIC}
    ci_check;

    echo "Validate test data w/ v${STABLE} CLI matches expected data created BEFORE upgrading cluster + CLI to v${PRERELEASE}"
    fluvio consume -B -d ${STABLE_TOPIC} | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c stable-cli-stable-topic.checksum; then
        echo "${STABLE_TOPIC} topic validated with v${STABLE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat stable-cli-stable-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    # Exercise older topics
    cat data2.txt.tmp | fluvio produce ${STABLE_MINUS_ONE_TOPIC}
    ci_check;

    echo "Validate v${STABLE_MINUS_ONE_TOPIC} test data w/ ${STABLE} CLI matches expected data AFTER upgrading cluster + CLI to v${STABLE}"
    fluvio consume -B -d ${STABLE_MINUS_ONE_TOPIC} | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c stable-cli-stable-minus-one-topic.checksum; then
        echo "${STABLE_MINUS_ONE_TOPIC} topic validated with v${STABLE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat stable-cli-stable-topic.checksum | awk '{print $1}')"
        exit 1
    fi

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

    cat data3.txt.tmp | fluvio produce ${PRERELEASE_TOPIC}
    ci_check;

    echo "Validate test data w/ v${PRERELEASE} CLI matches expected data AFTER upgrading cluster + CLI to v${PRERELEASE}"
    $FLUVIO_BIN consume -B -d ${PRERELEASE_TOPIC} | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c prerelease-cli-prerelease-topic.checksum; then
        echo "${PRERELEASE_TOPIC} topic validated with v${PRERELEASE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat prerelease-cli-prerelease-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    # Exercise older topics
    cat data3.txt.tmp | fluvio produce ${STABLE_TOPIC}
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

    cat data3.txt.tmp | fluvio produce ${STABLE_MINUS_ONE_TOPIC}
    ci_check;

    echo "Validate v${STABLE_MINUS_ONE_TOPIC} test data w/ ${PRERELEASE} CLI matches expected data AFTER upgrading cluster + CLI to v${PRERELEASE}"
    $FLUVIO_BIN consume -B -d ${STABLE_MINUS_ONE_TOPIC} | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c prerelease-cli-stable-minus-one-topic.checksum; then
        echo "${STABLE_MINUS_ONE_TOPIC} topic validated with v${PRERELEASE} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat prerelease-cli-stable-minus-one-topic.checksum | awk '{print $1}')"
        exit 1
    fi

}

# Create 3 base data files and calculate checksums for the expected states of each of our testing topics
# Build produce/consume with generated data to validate integrity across upgrades
function create_test_data() {
    local TEST_DATA_BYTES=${TEST_DATA_BYTES:-100}

    # The baseline files
    for BASE in {1..3}
    do
        echo "Create the baseline file \#${BASE}"
        local RANDOM_DATA=$(tr -cd '[:alnum:]' < /dev/urandom | fold -w${TEST_DATA_BYTES} | head -n1)
        echo ${RANDOM_DATA} | tee -a data${BASE}.txt.tmp
    done

    # Round 1
    # Stable-1 
    echo "Create expected topic checksum for Stable-1 cli x Stable-1 topic"
    cat data1.txt.tmp | shasum | tee -a stable-minus-one-cli-stable-minus-one-topic.checksum

    # Round 2
    # Topic 2 expected output
    echo "Create expected topic contents for Stable cli x Stable-1 topic"
    cat data1.txt.tmp data2.txt.tmp | tee -a stable-cli-stable-minus-one-topic.txt.tmp
    echo "Create expected topic checksum for Stable cli x Stable-1 topic"
    cat stable-cli-stable-minus-one-topic.txt.tmp | shasum | tee -a stable-cli-stable-minus-one-topic.checksum

    ## Topic 2 expected output
    cat data2.txt.tmp | shasum | tee -a stable-cli-stable-topic.checksum

    # Round 3
    # Topic 1 expected output
    echo "Create expected topic contents for Prerelease cli x Stable-1 topic"
    cat data1.txt.tmp data2.txt.tmp data3.txt.tmp | tee -a prerelease-cli-stable-minus-one-topic.txt.tmp
    echo "Create expected topic checksum for Prerelease cli x Stable-1 topic"
    cat prerelease-cli-stable-minus-one-topic.txt.tmp | shasum | tee -a prerelease-cli-stable-minus-one-topic.checksum

    # Topic 2 expected output
    echo "Create expected topic contents for Prerelease cli x Stable topic"
    cat data2.txt.tmp data3.txt.tmp | tee -a prerelease-cli-stable-topic.txt.tmp
    echo "Create expected topic checksum for Prerelease cli x Stable topic"
    cat prerelease-cli-stable-topic.txt.tmp | shasum | tee -a prerelease-cli-stable-topic.checksum

    # Topic 3 expected output
    echo "Create expected topic checksum for Prerelease cli x Prerelease topic"
    cat data3.txt.tmp | shasum | tee -a prerelease-cli-prerelease-topic.checksum
}

function main() {
    # Change to this script's directory 
    pushd "$(dirname "$(readlink -f "$0")")" > /dev/null

    cleanup;
    create_test_data;

    echo "Create cluster @ stable v${STABLE_MINUS_ONE}. Create and validate data."
    validate_cluster_out_of_date_stable;

    echo "Update cluster to stable v${STABLE}. Create and validate data."
    validate_upgrade_cluster_to_stable;

    echo "Update cluster to prerelease v${PRERELEASE}"
    validate_upgrade_cluster_to_prerelease;

    cleanup;

    # Change back to original directory
    popd > /dev/null
}

main;