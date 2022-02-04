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

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"

    MESSAGE_W_HTML_STR='"&"'
    export MESSAGE_W_HTML_STR

    MULTILINE_MESSAGE="$MESSAGE\n$MESSAGE_W_HTML_STR"
    export MULTILINE_MESSAGE
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
}

# Create topic
@test "Create a topic" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_2"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_3"
    assert_success
}

# Produce message 
@test "Produce message" {
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    run bash -c 'echo "$MESSAGE_W_HTML_STR" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_2"'
    run bash -c 'echo -e "$MULTILINE_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_3"'
    assert_success
}

# Consume message and compare message
# Warning: Adding anything extra to the `debug_msg` skews the message comparison
@test "Consume message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d

    assert_output --partial "$MESSAGE"
    assert_success
}

# Validate that using format doesn't introduce HTML escaping
# https://github.com/infinyon/fluvio/issues/1628
@test "Consume message using format" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_2" --format "{{value}}" -B -d
    assert_output "$MESSAGE_W_HTML_STR"
    assert_success
}

# Validate that consume --tail 1, returns only the last record
@test "Consume with tail" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_3" --tail 1 -d
    assert_output "$MESSAGE_W_HTML_STR"
    assert_success
}
