#!/usr/bin/env bats

load test_helper/fluvio_dev.bash
load test_helper/tools_check.bash
load test_helper/setup_k8_cluster.bash
load test_helper/random_string.bash

setup_file() {
    TABLE_NAME="$(random_string)"
    export TABLE_NAME
    debug_msg "Table name: $TABLE_NAME"
}

# Create table
@test "Create table" {
    skip "Creating tables currently broken"
    run "$FLUVIO_BIN" table create "$TABLE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 0 ]
}

# Create table - Negative test
@test "Attempt to create a table with same name" {
    run "$FLUVIO_BIN" table create "$TABLE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 1 ]
}

# List table
@test "List table" {
    run "$FLUVIO_BIN" table list
    [ "$status" -eq 0 ]
}

# Delete table
@test "Delete table" {
    skip "Deleting tables currently broken"
    run "$FLUVIO_BIN" table delete "$TABLE_NAME"
    [ "$status" -eq 0 ]
}

# Delete table - Negative test
@test "Attempt to delete a table that doesn't exist" {
    run "$FLUVIO_BIN" table delete "$TABLE_NAME"
    [ "$status" -eq 1 ]
    [[ "${lines[3]}" =~ "TableNotFound" ]]
}