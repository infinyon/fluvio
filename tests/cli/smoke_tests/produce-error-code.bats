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
    run rm $TOPIC_NAME.txt
}

# Create topic
@test "Create a topic SPU error code" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
}

# Produce message 
@test "Produce message with SPU error code" {
    run bash -c "yes a | tr -d "\n" |head -c 10000000 > $TOPIC_NAME.txt"
    run bash -c 'timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 50000000 --raw --file $TOPIC_NAME.txt'
    assert_failure
}
