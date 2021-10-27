#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/../test_helper/fluvio_dev.bash
load "$BATS_TEST_DIRNAME"/../test_helper/tools_check.bash
load "$BATS_TEST_DIRNAME"/../test_helper/setup_k8_cluster.bash
load "$BATS_TEST_DIRNAME"/../test_helper/random_string.bash

setup_file() {
    TOPIC_NAME=$(random_string)
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"
}

teardown_file() {
    run "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
}

# Create topic
@test "Create a topic" {
    debug_msg "topic: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    [ "$status" -eq 0 ]
}

# Produce message 
@test "Produce message" {
    #run bash -c "echo \"$MESSAGE\" | \"$FLUVIO_BIN\" produce \"$TOPIC_NAME\""
    produce_w_pipe() {
        echo "$MESSAGE" | "$FLUVIO_BIN" produce "$TOPIC_NAME"
    }

    run produce_w_pipe
    [ "$status" -eq 0 ]
}

# Consume message and compare message
# Warning: Adding anything extra to the `debug_msg` skews the message comparison
@test "Consume message" {
    debug_msg "$MESSAGE"
    run "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d

    debug_msg "${lines[0]}"

    [[ "${lines[0]}" =~ $MESSAGE ]]
    [ "$status" -eq 0 ]
}