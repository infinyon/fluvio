#!/usr/bin/env bats

load test_helper/fluvio_dev.bash
load test_helper/tools_check.bash
load test_helper/setup_k8_cluster.bash
load test_helper/random_string.bash

setup_file() {
    TOPIC_NAME=$(random_string)
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"
}

# Create topic
@test "Create a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic create "$TOPIC_NAME" 
    #debug_msg "command $BATS_RUN_COMMAND" # This doesn't do anything.
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 0 ]
}

# Create topic - Negative test
@test "Attempt to create a topic with same name" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Topic already exists" ]
}

# Describe topic
@test "Describe a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic describe "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 0 ]
}

# Delete topic
@test "Delete a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 0 ]
}

# Delete topic - Negative test 
@test "Attempt to delete a topic that doesn't exist" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Topic not found" ]
}