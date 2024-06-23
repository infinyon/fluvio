#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

# This test needs 2 SPUs running
setup_file() {
    TOPIC_NAME=$(random_string)
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"
}

# Create topic
@test "Create a topic" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
}

# Add partitions to topic
@test "Add 2 new partitions to the topic" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    debug_msg "Add partitions to topic"
    run timeout 15s "$FLUVIO_BIN" topic add-partition "$TOPIC_NAME" -c 2

    assert_line --index 0 "added new partitions to topic: "\"$TOPIC_NAME\"""
    assert_line --partial --index 1 "PARTITION  SPU"
    assert_line --partial --index 2 "1          5002"
    assert_line --partial --index 3 "2          5001"

    assert_success
}

# List partitions
@test "List partitions should have 3 partitions" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    sleep 2
    debug_msg "List partitions"
    run timeout 15s "$FLUVIO_BIN" partition list

    #TODO: filter without grep when fluvio partition list has a filter option
    run bash -c 'timeout 15s "$FLUVIO_BIN" partition list | grep "$TOPIC_NAME"'

    assert_line --partial --index 0 "$TOPIC_NAME  0          5001"
    assert_line --partial --index 1 "$TOPIC_NAME  1          5002"
    assert_line --partial --index 2 "$TOPIC_NAME  2          5001"
    
    assert_success
}

