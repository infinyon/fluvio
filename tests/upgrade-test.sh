#!/usr/bin/env bash

# The strategy for this test is to write to the cluster and roll forward
# with release upgrades
# We track the previous release, current release and the prerelease
#
# We create new topic per round.
# We'll produce and consume from all test topics per round.
# Content is verified via checksum

set -exu
#set -e

echo command: $0 $*

STABLE_MINUS_ONE=${1:?Starting cluster version [pos 1]}
STABLE=${2:?Second cluster version [pos 2]}
PRERELEASE=${3:-$(cat ../VERSION)-$(git rev-parse HEAD)}

CI_SLEEP=${CI_SLEEP:-10}
CI=${CI:-}

# Change to this script's directory 
pushd "$(dirname "$(readlink -f "$0")")" > /dev/null

function cleanup() {
    echo Clean up test data
    rm -f --verbose ./*.txt.tmp;
    rm -f --verbose ./*.checksum;
    echo Delete cluster if possible
    fluvio cluster delete || true
}

function ci_check() {
    if [[ ! -z "$CI" ]];
    then
        echo "CI, pausing for ${CI_SLEEP} second";
        w | head -1
        sleep ${CI_SLEEP};
    fi
}

function round1() {

    create_test_data;

    echo Install ${STABLE_MINUS_ONE} CLI 
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=${STABLE_MINUS_ONE} bash

    echo Start ${STABLE_MINUS_ONE} cluster
    fluvio cluster start
    ci_check;

    fluvio version
    ci_check;

    echo Create round 1 topic
    fluvio topic create round1-topic 
    ci_check;

    cat data1.txt.tmp | fluvio produce round1-topic
    ci_check;

    echo Validate test data matches expected data before upgrade
    fluvio consume -B -d round1-topic | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c round1-topic1.checksum; then
        echo Round 1 topic 1 validated
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat round1-topic1.checksum | awk '{print $1}')"
        exit 1
    fi
}

function round2() {

    echo "Install ${STABLE} CLI"
    curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=${STABLE} bash

    fluvio cluster upgrade
    ci_check;

    fluvio version
    ci_check;

    echo Create round 2 topic
    fluvio topic create round2-topic 
    ci_check;

    cat data2.txt.tmp | fluvio produce round2-topic
    ci_check;

    echo Validate test data matches expected data before upgrade
    fluvio consume -B -d round2-topic | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c round2-topic2.checksum; then
        echo Round 2 topic 2 validated
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat round2-topic2.checksum | awk '{print $1}')"
        exit 1
    fi

    # Exercise older topics
    cat data2.txt.tmp | fluvio produce round1-topic
    ci_check;

    echo "Validate test data matches expected data before upgrade"
    fluvio consume -B -d round1-topic | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c round2-topic1.checksum; then
        echo "Round 2 topic 1 validated"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat round2-topic2.checksum | awk '{print $1}')"
        exit 1
    fi

}

function round3() {

    if [[ ! -z "$CI" ]];
    then
        echo "We're in CI. Build and test the dev image"
        pushd ..
        make RELEASE=release TARGET=x86_64-unknown-linux-musl build_test
        make RELEASE=release minikube_image
        
        FLUVIO_BIN="$(pwd)/target/x86_64-unknown-linux-musl/release/fluvio"
        $FLUVIO_BIN cluster upgrade --chart-version=${PRERELEASE} --develop
        popd
    else 
        echo "Build and test the latest published dev image"
        echo "Install prerelease CLI"
        curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=latest bash
        FLUVIO_BIN=`which fluvio`
        $FLUVIO_BIN cluster upgrade --chart-version=${PRERELEASE}
    fi

    ci_check;

    $FLUVIO_BIN version
    ci_check;

    echo "Create round 3 topic"
    $FLUVIO_BIN topic create round3-topic
    ci_check;

    cat data3.txt.tmp | fluvio produce round3-topic
    ci_check;

    echo "Validate test data matches expected data before upgrade"
    $FLUVIO_BIN consume -B -d round3-topic | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c round3-topic3.checksum; then
        echo "Round 3 topic 3 validated"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat round3-topic3.checksum | awk '{print $1}')"
        exit 1
    fi

    # Exercise older topics
    cat data3.txt.tmp | fluvio produce round2-topic
    ci_check;

    echo "Validate test data matches expected data before upgrade"
    $FLUVIO_BIN consume -B -d round2-topic | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c round3-topic2.checksum; then
        echo "Round 3 topic 2 validated"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat round3-topic2.checksum | awk '{print $1}')"
        exit 1
    fi

    cat data3.txt.tmp | fluvio produce round1-topic
    ci_check;

    echo "Validate test data matches expected data before upgrade"
    $FLUVIO_BIN consume -B -d round1-topic | tee output.txt.tmp
    ci_check;

    if cat output.txt.tmp | shasum -c round3-topic1.checksum; then
        echo "Round 3 topic 1 validated"
    else
        echo "Got: $(cat output.txt.tmp | awk '{print $1}')"
        echo "Expected: $(cat round3-topic1.checksum | awk '{print $1}')"
        exit 1
    fi

}

function create_test_data() {

    # Create 3 base data files, checksums.
    # Build produce/consume with generated data to validate integrity across upgrades

    TEST_DATA_BYTES=${TEST_DATA_BYTES:-100}

    # The baseline files
    for BASE in {1..3}
    do
        echo "Create the baseline file \#${BASE}"
        RANDOM_DATA=$(tr -cd '[:alnum:]' < /dev/urandom | fold -w${TEST_DATA_BYTES} | head -n1)
        echo ${RANDOM_DATA} | tee -a data${BASE}.txt.tmp
    done

    # Round 1
    # Topic 1 expected output
    cat data1.txt.tmp | shasum | tee -a round1-topic1.checksum

    # Round 2
    # Topic 2 expected output
    echo "Create expected topic contents for topic 1 in round 2"
    cat data1.txt.tmp data2.txt.tmp | tee -a round2-topic1.txt.tmp
    echo "Create expected topic checksum for topic 1 in round 2"
    cat round2-topic1.txt.tmp | shasum | tee -a round2-topic1.checksum

    ## Topic 2 expected output
    cat data2.txt.tmp | shasum | tee -a round2-topic2.checksum

    # Round 3
    # Topic 1 expected output
    echo "Create expected topic contents for topic 1 in round 3"
    cat data1.txt.tmp data2.txt.tmp data3.txt.tmp | tee -a round3-topic1.txt.tmp
    echo "Create expected topic checksum for topic 1 in round 3"
    cat round3-topic1.txt.tmp | shasum | tee -a round3-topic1.checksum

    # Topic 2 expected output
    echo "Create expected topic contents for topic 2 in round 3"
    cat data2.txt.tmp data3.txt.tmp | tee -a round3-topic2.txt.tmp
    echo "Create expected topic checksum for topic 2 in round 3"
    cat round3-topic2.txt.tmp | shasum | tee -a round3-topic2.checksum

    # Topic 3 expected output
    cat data3.txt.tmp | shasum | tee -a round3-topic3.checksum
}

cleanup;

echo "First ${STABLE_MINUS_ONE}"
round1;

echo "then ${STABLE}"
round2;

echo "finally $(cat ../VERSION)-$(git rev-parse HEAD)"
round3;

cleanup;

# Change back to original directory
popd > /dev/null