#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    SMARTMODULE_NAME=$(random_string)
    export SMARTMODULE_NAME
    debug_msg "SmartModule name: $SMARTMODULE_NAME"
}

# Create smartmodule
# Currently just using an empty file
@test "Create smartmodule" {
    run timeout 15s "$FLUVIO_BIN" smartmodule create "$SMARTMODULE_NAME" --wasm-file "$(mktemp)"
    debug_msg "status: $status"
    echo "cmd: $BATS_RUN_COMMAND"
    debug_msg "output: ${lines[0]}"
    assert_output "smartmodule \"$SMARTMODULE_NAME\" has been created."
}

# Create smartmodule - Negative test
@test "Attempt to create a smartmodule with same name" {
    skip "Smartmodule creation doesn't fail w/ same names at the moment"
    run timeout 15s "$FLUVIO_BIN" smartmodule create "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    echo "cmd: $BATS_RUN_COMMAND"
    assert_failure
    assert_output --partial "Smartmodule already exists"
}

# Describe smartmodule
@test "Describe smartmodule" {
    skip "Describe is not yet implemented"
    run timeout 15s "$FLUVIO_BIN" smartmodule describe "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: $output"
    assert_success
}

# List smartmodule
@test "List smartmodule" {
    run timeout 15s "$FLUVIO_BIN" smartmodule list
    debug_msg "status: $status"
    debug_msg "output: $output"
    assert_success
}

# Delete smartmodule
@test "Delete smartmodule" {
    run timeout 15s "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete smartmodule - Negative test
@test "Attempt to delete a smartmodule that doesn't exist" {
    run timeout 15s "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[3]}"
    assert_failure
    assert_output --partial "SmartModule not found"
}
