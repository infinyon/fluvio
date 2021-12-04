#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    TABLE_FORMAT_NAME="testtable"
    export TABLE_FORMAT_NAME
    TABLE_FORMAT_CONFIG="$TEST_HELPER_DIR/test-table-format-config.yml"
    export TABLE_FORMAT_CONFIG
    debug_msg "TableFormat name: $TABLE_FORMAT_NAME"
}

# Create table-format
@test "Create table-format" {
    run timeout 15s "$FLUVIO_BIN" table-format create --config "$TABLE_FORMAT_CONFIG"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create table-format - Negative test
@test "Attempt to create a table-format with same name" {
    run timeout 15s "$FLUVIO_BIN" table-format create --config "$TABLE_FORMAT_CONFIG"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_output --partial "TableFormat already exists"
}

# List table-format
@test "List table-format" {
    run timeout 15s "$FLUVIO_BIN" table-format list
    assert_success
}

# Delete table-format
@test "Delete table-format" {
    run timeout 15s "$FLUVIO_BIN" table-format delete "$TABLE_FORMAT_NAME"
    assert_success
}

# Delete table-format - Negative test
@test "Attempt to delete a table-format that doesn't exist" {
    run timeout 15s "$FLUVIO_BIN" table-format delete "$TABLE_FORMAT_NAME"
    assert_output --partial "TableFormat not found"
}
