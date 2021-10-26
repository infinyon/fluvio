#!/usr/bin/env bats

setup_file() {
    load test_helper/tools_check.bash
    load test_helper/setup_k8_cluster.bash
}

setup() {
    load test_helper/random_string.bash
    export TABLE_NAME="$RANDOM_STRING"
}

# Create table
@test "Create table" {
    run "$FLUVIO_BIN" table create "$TABLE_NAME"
}

# Create table - Negative test
@test "Attempt to create a table with same name" {
    run -1 "$FLUVIO_BIN" table create "$TABLE_NAME"
}

# List table
@test "List table" {
    run "$FLUVIO_BIN" table list
}

# Delete table
@test "Delete table" {
    run "$FLUVIO_BIN" table delete "$TABLE_NAME"
}

# Delete table - Negative test
@test "Attempt to delete a table that doesn't exist" {
    run -1 "$FLUVIO_BIN" table delete "$TABLE_NAME"
}