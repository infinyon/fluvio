#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/../test_helper/tools_check.bash
load "$BATS_TEST_DIRNAME"/../test_helper/fluvio_dev.bash
load "$BATS_TEST_DIRNAME"/../test_helper/bats-support/load.bash
load "$BATS_TEST_DIRNAME"/../test_helper/bats-assert/load.bash

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
    assert_success
}

# Create topic - Negative test
@test "Attempt to create a topic with same name" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "Topic already exists"
}

# Describe topic
@test "Describe a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic describe "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete topic
@test "Delete a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete topic - Negative test 
@test "Attempt to delete a topic that doesn't exist" {
    debug_msg "Topic name: $TOPIC_NAME"
    run "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "Topic not found"
}