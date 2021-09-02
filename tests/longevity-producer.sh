#!/usr/bin/env bash

trap 'report_error $? $LINENO' EXIT

function report_error() {
    if [ "$1" != "0" ]; then
        echo "❌ Exit code $1"
        echo "Runtime progress (seconds): $SECONDS / $TOTAL_TEST_TIME"
    fi
}

readonly DEBUG=${DEBUG:-false}

readonly HOUR_IN_SECONDS=3600
readonly TEN_MIN_IN_SECONDS=600

# This var controls the expected test duration
readonly TOTAL_TEST_TIME=${TOTAL_TEST_TIME:-$TEN_MIN_IN_SECONDS}
readonly PAYLOAD_SIZE=${PAYLOAD_SIZE:-1000}
readonly NEW_TOPIC_NAME=longevity-new
readonly EXISTING_TOPIC_NAME=longevity-existing
readonly PRODUCER_RATE=${PRODUCER_RATE:-20} # 20 msg/s => 72K msg/hour

# On Mac, use 'greadlink' instead of 'readlink'
if [[ "$(uname)" == "Darwin" ]]; then
    [[ -x "$(command -v greadlink)" ]] || brew install coreutils
    readonly READLINK="greadlink"
else
    readonly READLINK="readlink"
fi

readonly FLUVIO_BIN=$(${READLINK} -f ${FLUVIO_BIN:-"$(which fluvio)"})

### Setup
# Calculate time into the future (Default to one hour)
# Configure the number of records to send per second
# Configure the payload length
# Connect to cluster
function setup() {

    echo "Test duration is ${TOTAL_TEST_TIME} seconds."

    if ${DEBUG} ; then
        echo "DEBUG verbosity on"
        set -exu
    else
        set -eu
        echo "Test output will be silent after setup for test duration"
    fi


    # Start a cluster
    $FLUVIO_BIN cluster start --image-version latest

    # Create a topic to delete at the end
    #$FLUVIO_BIN topic create $NEW_TOPIC_NAME || true

    # Create a topic, if it doesn't exist, that should stick around for future tests
    $FLUVIO_BIN topic create $EXISTING_TOPIC_NAME || true

    # TODO: Load any topic data into the topic

    # TODO: Announce the test vars

    echo "Setup complete"
}

## If we've run out of time then test is done
## Otherwise run the test function, and then sleep for a second
## Sleep for a second
function longevity_loop() {
    while [ $SECONDS -lt $TOTAL_TEST_TIME ]; do

        # Produce a message.
        # Provide current second tick as ID
        #test_produce $NEW_TOPIC_NAME $SECONDS;

        # Uncomment when running outside of github
        test_produce $EXISTING_TOPIC_NAME $SECONDS;

        sleep 1
    done
}

# WARNING: This test MIGHT need some CI-awareness checks, bc produce could be too aggressive for low-resource environment
## Test function
## Loop for the # of records we want to send
## Send the record
function test_produce() {
    TOPIC_NAME=$1
    MESSAGE_ID=$(($2+1))
    local TIMESTAMP_EPOCH=$(date +%s)
    local TEST_DATA=$(shuf -zer -n${PAYLOAD_SIZE}  {A..Z} {a..z} {0..9} | tr -d '\0')
    #local TEST_DATA="Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
    
    MSG_NUM=1;
    while [ $MSG_NUM -lt $(($PRODUCER_RATE+1)) ]
    do
        # Timestamp, CurrentMsg#/ProducerPerSecond, SecondTimestamp/TotalSeconds, TestData
        PRODUCER_PAYLOAD="$TIMESTAMP_EPOCH,${MSG_NUM}/${PRODUCER_RATE},${MESSAGE_ID}/${TOTAL_TEST_TIME},${TEST_DATA}";
        echo ${PRODUCER_PAYLOAD} | $FLUVIO_BIN produce ${TOPIC_NAME} 
        let MSG_NUM=MSG_NUM+1;
    done
}

function cleanup() {

    # TODO: Save the data from off the kubernetes pod


    # Delete the topic.
    #$FLUVIO_BIN topic delete $NEW_TOPIC_NAME || true

    # Leave the existing topic alone so it can be used in the next test
    # In Github runner, we'll recreate and reload data,
    #$FLUVIO_BIN topic delete $EXISTING_TOPIC_NAME || true
    :
}


function main() {
    setup;
    longevity_loop;
    cleanup;
    echo "✅ Test passed"
}

main;