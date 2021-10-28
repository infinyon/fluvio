#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/../test_helper/fluvio_dev.bash
load "$BATS_TEST_DIRNAME"/../test_helper/tools_check.bash
load "$BATS_TEST_DIRNAME"/../test_helper/setup_k8_cluster.bash

setup_file() {
    CONNECTOR_CONFIG="$BATS_TEST_DIRNAME/../test_helper/test-connector-config.yml"
    export CONNECTOR_CONFIG
    INVALID_CONFIG=$(mktemp)
    export INVALID_CONFIG
    CONNECTOR_NAME="my-test-mqtt"
    export CONNECTOR_NAME
    CONNECTOR_TOPIC="my-mqtt"
    export CONNECTOR_TOPIC
}

teardown_file() {
    # Delete connector's topic 
    run "$FLUVIO_BIN" topic delete "$CONNECTOR_TOPIC"
}

# Create connector
@test "Create test connector" {
    run "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
    [ "$status" -eq 0 ]
}

# Create same connector - Negative test
@test "Attempt to create test connector again" {
    run "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Topic already exists" ]
}

# Create connector w/ invalid config - Negative test
@test "Attempt to create test connector with invalid config" {
    run "$FLUVIO_BIN" connector create --config "$INVALID_CONFIG"
    [ "$status" -eq 1 ]
}

# List connector
@test "List test connector" {
    run "$FLUVIO_BIN" connector list
    [ "$status" -eq 0 ]
}

# Delete connector
@test "Delete test connector" {
    run "$FLUVIO_BIN" connector delete $CONNECTOR_NAME 
    [ "$status" -eq 0 ]
}

# Delete connector - Negative test
@test "Attempt to delete test connector that doesn't exist" {
    run "$FLUVIO_BIN" connector delete $CONNECTOR_NAME 
    [ "$status" -eq 1 ]
}

# This is assuming the previous test connector config has `create_topic: true`
# Create connector w/ but topic already exists
@test "Attempt to create test connector that creates topics, but the topic exists" {
    run "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Topic already exists" ]
}