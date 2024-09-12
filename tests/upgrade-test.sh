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

set -E
#set -ex

# On Mac, use 'greadlink' instead of 'readlink'
if [[ "$(uname)" == "Darwin" ]]; then
    [[ -x "$(command -v greadlink)" ]] || brew install coreutils
    readonly READLINK="greadlink"
else
    readonly READLINK="readlink"
fi

readonly STABLE=${1:-stable}
readonly PRERELEASE=${2:-$(cat VERSION)-$(git rev-parse HEAD)}
readonly CI_SLEEP=${CI_SLEEP:-2}
readonly CI=${CI:-}
readonly STABLE_TOPIC=${STABLE_TOPIC:-stable}
readonly PRERELEASE_TOPIC=${PRERELEASE_TOPIC:-prerelease}
readonly USE_LATEST=${USE_LATEST:-}
readonly FLUVIO_BIN=$(${READLINK} -f ${FLUVIO_BIN:-"$(which fluvio)"})
readonly FVM_BIN=$(${READLINK} -f ${FVM_BIN:-"~/.fvm/bin/fvm"})
readonly FLUVIO_MODE=${FLUVIO_MODE:-"k8"}

# Change to this script's directory 
pushd "$(dirname "$(${READLINK} -f "$0")")" > /dev/null

function cleanup() {
    echo Clean up test data
    rm -fv ./*.txt.tmp;
    rm -fv ./*.checksum;
 #   echo Delete cluster if possible
 #   $FLUVIO_BIN cluster delete || true
 #   $FLUVIO_BIN cluster delete --sys || true
}

# If we're in CI, we want to slow down execution
# to give CPU some time to rest, so we don't time out
function ci_check() {
        :
}

# This function is intended to be run second after the Stable-1 validation
# We install the Stable CLI, and upgrade the existing cluster
# A brand new topic is created, and we do a produce + consume and validate the checksum of the output on that topic
# Then we produce + consume on the Stable-1 topic and validate the checksums on that topic
function validate_cluster_stable() {

    echo "Install (current stable) CLI"
    unset VERSION

    curl -fsS https://hub.infinyon.cloud/install/install.sh?ctx=ci | bash
    
    ~/.fvm/bin/fvm install stable | tee /tmp/installer.output 
    STABLE_VERSION=$(cat /tmp/installer.output | grep "fluvio@" | awk '{print $4}' | cut -b 8-)

    local STABLE_FLUVIO=${HOME}/.fluvio/bin/fluvio

    # This is more for ensuring local dev will pass this test if you've changed your channel
    echo "Switch to \"stable\" channel CLI"
    ~/.fvm/bin/fvm switch stable

    echo "Installing stable fluvio cluster"
    if [[ "$FLUVIO_MODE" == "local" ]]; then
	$STABLE_FLUVIO cluster start --local
    else
	$STABLE_FLUVIO cluster start --k8
    fi

    ci_check;

    # Baseline: CLI version and platform version are expected to be the same

    $STABLE_FLUVIO version
    validate_cli_version $STABLE_FLUVIO $STABLE_VERSION
    validate_platform_version $STABLE_FLUVIO $STABLE_VERSION
    ci_check;


    echo "Create test topic: ${STABLE_TOPIC}"
    $STABLE_FLUVIO topic create ${STABLE_TOPIC} 
    # $STABLE_FLUVIO topic create ${STABLE_TOPIC}-delete 
    ci_check;

    echo "Create mirror"
    $STABLE_FLUVIO remote register stable-remote

    # Validate consume on topic before produce
    # https://github.com/infinyon/fluvio/issues/1819
    echo "Validate consume on \"${STABLE_TOPIC}\" before producing"
    $STABLE_FLUVIO consume -B -d ${STABLE_TOPIC} 2>/dev/null
    ci_check;

    echo "Producing test data to ${STABLE_TOPIC}"
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
    ci_check;

}

# This function is intended to be run last after the Stable validation
# We install the Prerelease CLI (either the dev prerelease, or compiled if we're in CI), and upgrade the existing cluster
# Another brand new topic is created, and we do a produce + consume and validate the checksum of the output on that topic
# Then we produce + consume on the Stable + Stable-1 topic and validate the checksums on each of those topics
function validate_upgrade_cluster_to_prerelease() {

    local FLUVIO_BIN_ABS_PATH=$(${READLINK} -f $FLUVIO_BIN)
    local TARGET_VERSION=${PRERELEASE}

    # Change dir to get access to Helm charts
    pushd ..
    if [[ ! -z "$USE_LATEST" ]];
    then
        # Use the "latest" fluvio channel
        echo "Switch to \"latest\" channel CLI"
        FLUVIO_BIN_ABS_PATH=${HOME}/.fluvio/bin/fluvio

        ~/.fvm/bin/fvm install latest | tee /tmp/installer.output 
        # expectd output fvm current => 0.11.0-dev-1+hash (latest)
        DEV_VERSION=$(~/.fvm/bin/fvm current | awk '{print $1}')
        TARGET_VERSION=${DEV_VERSION:0:${#DEV_VERSION}-41}

        echo "Installed CLI version ${DEV_VERSION}"
        echo "Upgrading cluster to ${DEV_VERSION}"
        echo "Target Version ${TARGET_VERSION}"

        FLUVIO_IMAGE_TAG_STRATEGY=version-git \
        $FLUVIO_BIN_ABS_PATH cluster upgrade --force
        echo "Wait for SPU to be upgraded. sleeping 1 minute"

    else
        TARGET_VERSION=${PRERELEASE:0:${#PRERELEASE}-41}
        echo "Test local image v${PRERELEASE}"
        echo "Target Version ${TARGET_VERSION}"
        # This should use the binary that the Makefile set

        echo "Using Fluvio binary located @ ${FLUVIO_BIN_ABS_PATH}"
        $FLUVIO_BIN_ABS_PATH cluster upgrade --force --develop
    fi
    if [[ "$FLUVIO_MODE" == "local" ]]; then
	echo "Resuming local cluster"
	$FLUVIO_BIN_ABS_PATH cluster resume
    fi
    popd

    ci_check;

    # Validate that the development version output matches the expected version from installer output
    $FLUVIO_BIN_ABS_PATH version
    validate_cli_version $FLUVIO_BIN_ABS_PATH $TARGET_VERSION
    ci_check;

    validate_platform_version $FLUVIO_BIN_ABS_PATH $TARGET_VERSION
    ci_check;

    echo "Create test topic: ${PRERELEASE_TOPIC}"
    $FLUVIO_BIN_ABS_PATH topic create ${PRERELEASE_TOPIC}

    # Validate consume on topic before produce
    # https://github.com/infinyon/fluvio/issues/1819
    echo "Validate consume on \"${PRERELEASE_TOPIC}\" before producing"
    $FLUVIO_BIN_ABS_PATH consume -B -d ${PRERELEASE_TOPIC} 2>/dev/null

    #kubectl get pods
    #kubectl get spu

    echo "Producing test data to ${PRERELEASE_TOPIC}"
    cat data2.txt.tmp | $FLUVIO_BIN_ABS_PATH produce ${PRERELEASE_TOPIC}


    echo "Validate test data w/ v${TARGET_VERSION} CLI matches expected data AFTER upgrading cluster + CLI to v${TARGET_VERSION}"
    $FLUVIO_BIN_ABS_PATH consume -B -d ${PRERELEASE_TOPIC} 2>/dev/null | tee output.txt.tmp


    if cat output.txt.tmp | shasum -c prerelease-cli-prerelease-topic.checksum; then
        echo "${PRERELEASE_TOPIC} topic validated with v${TARGET_VERSION} CLI"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat prerelease-cli-prerelease-topic.checksum | awk '{print $1}')"
        exit 1
    fi

    # Validate consume on topic before produce
    # https://github.com/infinyon/fluvio/issues/1819
    echo "Validate consume on \"${STABLE_TOPIC}\" before producing"
    $FLUVIO_BIN_ABS_PATH consume -B -d ${STABLE_TOPIC} 2>/dev/null

    # Exercise older topics
    echo "Producing test data to ${STABLE_TOPIC}"
    cat data2.txt.tmp | $FLUVIO_BIN_ABS_PATH produce ${STABLE_TOPIC}

    echo "Validate v${STABLE} test data w/ ${TARGET_VERSION} CLI matches expected data AFTER upgrading cluster + CLI to v${TARGET_VERSION}"
    $FLUVIO_BIN_ABS_PATH consume -B -d ${STABLE_TOPIC} | tee output.txt.tmp


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


# Do I need to compare SHAs here?
function validate_cli_version() {
    CUR_FLUVIO_BIN=$1;
    FLUVIO_SEMVER=$2;

    # Get the fluvio version output
    # Parse out CLI version
    CLI_VERSION=$($CUR_FLUVIO_BIN version | grep "Fluvio CLI" | head -1 | awk '{print $4}')

    if [ "$CLI_VERSION" = "$FLUVIO_SEMVER" ]; then
      echo "✅ CLI Version verified: $CLI_VERSION";
    else
      echo "❌ CLI Version check failed";
      echo "Version reported by fluvio version: $CLI_VERSION";
      echo "Expected version : $FLUVIO_SEMVER";
      exit 1;
    fi

}

function validate_platform_version() {
    CUR_FLUVIO_BIN=$1;
    FLUVIO_SEMVER=$2;

    # Get the fluvio version output
    # Parse out Platform version
    PLATFORM_VERSION=$($CUR_FLUVIO_BIN version | grep "Fluvio Platform" | head -1 | awk '{print $4}')

    # Compare Platform == FLUVIO_SEMVER
    if [ "$PLATFORM_VERSION" = "$FLUVIO_SEMVER" ]; then
      echo "✅ Platform Version verified: $PLATFORM_VERSION";
    else
      echo "❌ Platform Version check failed";
      echo "Version reported by fluvio version: $PLATFORM_VERSION";
      echo "Expected version : $FLUVIO_SEMVER";
      exit 1;
    fi


}

# Create 2 base data files and calculate checksums for the expected states of each of our testing topics
# Build produce/consume with generated data to validate integrity across upgrades
function create_test_data() {
    local TEST_DATA_BYTES=${TEST_DATA_BYTES:-100}

    # The baseline files
    for BASE in {1..2}
    do
        echo "Create the baseline file #${BASE}"
        local RANDOM_DATA=$(shuf -zer -n${TEST_DATA_BYTES}  {A..Z} {a..z} {0..9})
        echo ${RANDOM_DATA} | tee -a data${BASE}.txt.tmp
        date
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

    ci_check;

    echo "Update cluster to prerelease v${PRERELEASE}"
    validate_upgrade_cluster_to_prerelease;

    cleanup;

    # Change back to original directory
    popd > /dev/null
}

main;

