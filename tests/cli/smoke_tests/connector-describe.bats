#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    CONNECTOR_CONFIG="$TEST_HELPER_DIR/connectors/test-connector-config.yaml"
    export CONNECTOR_CONFIG
    INVALID_CONFIG=$(mktemp)
    export INVALID_CONFIG
    CONNECTOR_NAME="my-test-connector"
    export CONNECTOR_NAME
    CONNECTOR_TOPIC="my-test-topic"
    export CONNECTOR_TOPIC
}

teardown_file() {
    # Delete connector's topic
    run timeout 15s "$FLUVIO_BIN" topic delete "$CONNECTOR_TOPIC"
    run timeout 15s "$FLUVIO_BIN" connector delete "$CONNECTOR_NAME"
}

# Create connector
@test "Create test connector and get test connector config" {
    run timeout 15s "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
    assert_success
    run timeout 15s "$FLUVIO_BIN" connector config "$CONNECTOR_NAME"
    assert_success
    assert_output "
---
version: latest
name: my-test-connector
type: test-connector
topic: my-test-connector-topic
create_topic: true
"
}
