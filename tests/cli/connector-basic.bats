#!/usr/bin/env bats

load test_helper/tools_check.bash
load test_helper/setup_k8_cluster.bash

setup() {
    export CONNECTOR_CONFIG="./test_helper/test-connector-config.yml"
    INVALID_CONFIG=$(mktemp)
    export INVALID_CONFIG
    export CONNECTOR_NAME="my-test-mqtt"
    export CONNECTOR_TOPIC="my-mqtt"
}

teardown_file() {
    # Delete connector's topic 
    run "$FLUVIO_BIN" topic delete "$CONNECTOR_TOPIC"
}

# Create connector
@test "Create test connector" {
    run "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
}

# Create same connector - Negative test
@test "Attempt to create test connector again" {
    run -1 "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
}

# Create connector w/ invalid config - Negative test
@test "Attempt to create test connector with invalid config" {
    run -1 "$FLUVIO_BIN" connector create --config "$INVALID_CONFIG"
}

# List connector
@test "List test connector" {
    run "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
}

# Delete connector
@test "Delete test connector" {
    run "$FLUVIO_BIN" connector delete $CONNECTOR_NAME 
}

# Delete connector - Negative test
@test "Attempt to delete test connector that doesn't exist" {
    run -1 "$FLUVIO_BIN" connector delete $CONNECTOR_NAME 
}

# This is assuming the previous test connector config has `create_topic: true`
# Create connector w/ but topic already exists
@test "Attempt to create test connector that creates topics, but the topic exists" {
    run -1 "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
}