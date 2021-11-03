#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/../test_helper/tools_check.bash
load "$BATS_TEST_DIRNAME"/../test_helper/fluvio_dev.bash
load "$BATS_TEST_DIRNAME"/../test_helper/bats-support/load.bash
load "$BATS_TEST_DIRNAME"/../test_helper/bats-assert/load.bash

setup_file() {
    TABLE_NAME="$(random_string)"
    export TABLE_NAME
    debug_msg "Table name: $TABLE_NAME"
}

# Create table
@test "Create table" {
    skip "Creating tables currently broken"
    run timeout 15s "$FLUVIO_BIN" table create "$TABLE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create table - Negative test
@test "Attempt to create a table with same name" {
    run timeout 15s "$FLUVIO_BIN" table create "$TABLE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
}

# List table
@test "List table" {
    run timeout 15s "$FLUVIO_BIN" table list
    assert_success
}

# Delete table
@test "Delete table" {
    skip "Deleting tables currently broken"
    run timeout 15s "$FLUVIO_BIN" table delete "$TABLE_NAME"
    assert_success
}

# Delete table - Negative test
@test "Attempt to delete a table that doesn't exist" {
    run timeout 15s "$FLUVIO_BIN" table delete "$TABLE_NAME"
    assert_failure
    assert_output --partial "TableNotFound"
}