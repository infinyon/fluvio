#!/usr/bin/env bash

set -exu

# Proof of concept. Might need to implement in Rust

# This longevity test will test a producer's ability to send data over a longer period of time

# We know we're going to be running for X seconds/minutes

# So we want to produce an upper limit of Y records per second

# The idea is that we'll always use the same topic and we'll send basically the same type of payload
# One record at a time until the upper limit is met
# Then we basically sit idle, waiting for the next tick

# Repeat until we've used up all of the time


readonly HOUR_IN_SECONDS=3600
readonly MIN_IN_SECONDS=60
readonly TOTAL_TEST_TIME=${MIN_IN_SECONDS}
readonly PAYLOAD_SIZE=1000
readonly TOPIC_NAME=longevity
readonly PRODUCER_RATE=10
readonly FLUVIO_BIN=~/.fluvio/bin/fluvio

###

### Setup
# Calculate time into the future (Default to one hour)
# Configure the number of records to send per second
# Configure the payload length
# Connect to cluster
function setup() {
    echo "Setup"

    # TODO: Login to Dev cluster
    # Create a topic

    $FLUVIO_BIN topic create $TOPIC_NAME || true

    # TODO: Announce the test vars
}

## If we've run out of time then test is done
## Otherwise run the test function, and then sleep for a second
## Sleep for a second
function longevity_loop() {
    while [ $SECONDS -lt $TOTAL_TEST_TIME ]; do

        # Produce a message.
        # Provide current second tick as ID
        test_produce $SECONDS;

        sleep 1
    done
}



## Test function
## Loop for the # of records we want to send
## Send the record
function test_produce() {
    MESSAGE_ID=$(($1+1))
    local TIMESTAMP_EPOCH=$(date +%s)
    local RANDOM_DATA=$(tr -cd '[:alnum:]' < /dev/urandom | fold -w${PAYLOAD_SIZE} | head -n1)
    
    MSG_NUM=1;
    while [ $MSG_NUM -lt $(($PRODUCER_RATE+1)) ]
    do
        # Timestamp, CurrentMsg#/ProducerPerSecond, SecondTimestamp/TotalSeconds, RandomData
        PRODUCER_PAYLOAD="$TIMESTAMP_EPOCH,${MSG_NUM}/${PRODUCER_RATE},${MESSAGE_ID}/${TOTAL_TEST_TIME},${RANDOM_DATA}";
        echo ${PRODUCER_PAYLOAD} | $FLUVIO_BIN produce ${TOPIC_NAME} 
        let MSG_NUM=MSG_NUM+1;
    done
}


function main() {
    setup;
    longevity_loop;
}

main;