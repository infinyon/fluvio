#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    CURRENT_DATE=$(date +%Y-%m)
    export CURRENT_DATE

    TOPIC_NAME=my-topic
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" cluster shutdown
}

@test "Cannot create a new topic" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create other-topic
    assert_output --partial "permission denied"
}

# Produce message
@test "Can produce message in existing topic" {
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
}

@test "Can consume message in existing topic" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d

    assert_output --partial "$MESSAGE"
    assert_success
}
