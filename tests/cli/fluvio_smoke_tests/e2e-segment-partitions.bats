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

    TOPIC_NAME_2=$(random_string)
    export TOPIC_NAME_2
    debug_msg "Topic name: $TOPIC_NAME_2"

    TOPIC_NAME_3=$(random_string)
    export TOPIC_NAME_3
    debug_msg "Topic name: $TOPIC_NAME_3"

    MESSAGE="$(random_string 1000)"
    export MESSAGE
    debug_msg "$MESSAGE"
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME_2"
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME_3"
}

@test "Create a topic" {
    debug_msg "topic: $TOPIC_NAME with segment size of 1024"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" --segment-size 1024
    assert_success

    debug_msg "topic: $TOPIC_NAME with segment size of 2048"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_2" --segment-size 2048
    assert_success

    debug_msg "topic: $TOPIC_NAME with segment size of 4096"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_3" --segment-size 4096
    assert_success
}

@test "Produce message" {
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'

    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_2"'
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_2"'
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_2"'

    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_3"'
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_3"'
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_3"'
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_3"'
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_3"'
    assert_success
}

@test "Check Input Argument Error" {
    run timeout 15s kubectl logs pod/fluvio-spg-main-0
    debug_msg "Checking for \'Invalid Argument\' Error in SPG logs"
    refute_output --partial 'message: "Invalid argument"'
}
