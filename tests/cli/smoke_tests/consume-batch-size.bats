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
@test "Create topics for test" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
}

@test "Produce message with batch large" {
    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 15000000 > $TOPIC_NAME.txt"
    run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 15000000 --file $TOPIC_NAME.txt --linger 30s'
    assert_success
}

# Consume message and compare message
@test "Consume message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d --maxbytes 16000000
    assert_output --partial "abcdefghijklmnopqrstuvwxyz"
    assert_success
}
