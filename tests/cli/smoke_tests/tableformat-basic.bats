#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    TABLEFORMAT_NAME="testtable"
    export TABLEFORMAT_NAME
    TABLEFORMAT_CONFIG="$TEST_HELPER_DIR/test-tableformat-config.yml"
    export TABLEFORMAT_CONFIG
    debug_msg "TableFormat name: $TABLEFORMAT_NAME"
}

# Create tableformat
@test "Create tableformat" {
    run timeout 15s "$FLUVIO_BIN" tableformat create --config "$TABLEFORMAT_CONFIG"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create tableformat - Negative test
@test "Attempt to create a tableformat with same name" {
    run timeout 15s "$FLUVIO_BIN" tableformat create --config "$TABLEFORMAT_CONFIG"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_output --partial "TableFormat already exists"
}

# List tableformat
@test "List tableformat" {
    run timeout 15s "$FLUVIO_BIN" tableformat list
    assert_success
}

# Delete tableformat
@test "Delete tableformat" {
    run timeout 15s "$FLUVIO_BIN" tableformat delete "$TABLEFORMAT_NAME"
    assert_success
}

# Delete tableformat - Negative test
@test "Attempt to delete a tableformat that doesn't exist" {
    run timeout 15s "$FLUVIO_BIN" tableformat delete "$TABLEFORMAT_NAME"
    assert_output --partial "TableFormat not found"
}
