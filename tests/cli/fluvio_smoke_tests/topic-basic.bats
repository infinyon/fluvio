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

# Create topic
@test "Create a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" 
    #debug_msg "command $BATS_RUN_COMMAND" # This doesn't do anything.
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create topic - Negative test
@test "Attempt to create a topic with same name" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "Topic already exists"
}

# Describe topic
@test "Describe a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic describe "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete topic
@test "Delete a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete topic - Negative test 
@test "Attempt to delete a topic that doesn't exist" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "Topic not found"
}

# Create topic with max partition size (dry run)
@test "Attempt to create topic with specified max partition size" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --max-partition-size 10Gb --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create topic with segment size (dry run)
@test "Attempt to create topic with specified segment size" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --segment-size "2 Ki" --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create topic with too small max partition size (dry run) - Negative test
@test "Attempt to create topic with too small max partition size" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --max-partition-size "10" --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "max_partition_size 10 is less than minimum 2048"
}

# Create topic with max partition size is less than segment size (dry run) - Negative test
@test "Attempt to create topic with max partition size smaller than segment size" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --segment-size "3 Ki" --max-partition-size "2 Ki" --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "max_partition_size 2048 is less than segment size 3072"
}