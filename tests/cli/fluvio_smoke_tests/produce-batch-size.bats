#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    TOPIC_NAME=$(random_string)
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    run rm $TOPIC_NAME-small.txt
    run rm $TOPIC_NAME-med.txt
    run rm $TOPIC_NAME-big.txt
}

# Create topic
@test "Create topics for test" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
}

# regression test issue https://github.com/infinyon/fluvio/issues/4161
@test "Produce message larger than batch size" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 2500000 > $TOPIC_NAME-small.txt"
    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 5000000 > $TOPIC_NAME-med.txt"
    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 15000000 > $TOPIC_NAME-big.txt"

    debug_msg small 25
    run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --max-request-size 100000000 --file $TOPIC_NAME-small.txt --raw --compression gzip'
    assert_success

    debug_msg med 50+1
    run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --max-request-size 100000000 --file $TOPIC_NAME-med.txt --raw --compression gzip'
    assert_success

    # should create a single batch for this record 
    debug_msg big 150
    run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --max-request-size 100000000 --file $TOPIC_NAME-big.txt --raw --compression gzip'
    assert_success
}

# This is to cover cases when the metadata + record size is larger than the batch size
@test "Produce message larger than batch size with loop" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi


    for x in $(seq 0 99); do
	debug_msg "Running test with $x characters"

	# Create the text file with 'x' number of characters
	run bash -c "yes abcdefghijklmnopqrstuvwxyz | head -c 49999$x > $TOPIC_NAME-loop.txt"

	debug_msg small 25
	run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --max-request-size 100000000 --file $TOPIC_NAME-loop.txt --raw --compression gzip'
	assert_success
    done
}

@test "Produce message larger then max request size" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
	skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
	skip "don't run on cluster stable version"
    fi
    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 14999000 > $TOPIC_NAME.txt"
    run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --max-request-size 15000000 --file $TOPIC_NAME.txt --raw --compression gzip'
    assert_success

    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 15000000 > $TOPIC_NAME.txt"
    run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --max-request-size 15000000 --file $TOPIC_NAME.txt --raw --compression gzip'
    assert_failure
}

