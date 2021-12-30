#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    CONNECTOR_CONFIG="$TEST_HELPER_DIR/connectors/3rd-party-test-connector-config.yaml"
    export CONNECTOR_CONFIG
    UNAUTHORIZED_CONNECTOR_CONFIG="$TEST_HELPER_DIR/connectors/3rd-party-test-connector-config-unauthorized.yaml"
    export UNAUTHORIZED_CONNECTOR_CONFIG
}

# Create connector
@test "Create test connector" {
    run timeout 15s "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
    assert_success
}

@test "Attempt to create test connector from an unauthorized source" {
    run timeout 15s "$FLUVIO_BIN" connector create --config "$UNAUTHORIZED_CONNECTOR_CONFIG"
    assert_success
    sleep 10
    run $FLUVIO_BIN connector list

    assert_output --partial "my-invalid-third-party-connector  Invalid"
}

teardown_file() {
    # Delete connector's topic
    run timeout 15s "$FLUVIO_BIN" delete delete my-invalid-third-party-connector
    run timeout 15s "$FLUVIO_BIN" delete delete my-third-party-connector
}
