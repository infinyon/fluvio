#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/../test_helper/tools_check.bash
load "$BATS_TEST_DIRNAME"/../test_helper/fluvio_dev.bash
load "$BATS_TEST_DIRNAME"/../test_helper/bats-support/load.bash
load "$BATS_TEST_DIRNAME"/../test_helper/bats-assert/load.bash

setup_file() {
    TOPIC_NAME=$(random_string)
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
}

# Create topic
@test "Create a topic" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
}

# Produce message 
@test "Produce message" {
    produce_w_pipe() {
        echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"
    }

    run produce_w_pipe
    assert_success
}

# Consume message and compare message
# Warning: Adding anything extra to the `debug_msg` skews the message comparison
@test "Consume message" {
    debug_msg "$MESSAGE"
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d

    debug_msg "${lines[0]}"

    assert_output --partial "$MESSAGE"
    assert_success
}