#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    SMART_MODULE_NAME=$(random_string)
    export SMART_MODULE_NAME
    debug_msg "SmartModule name: $SMART_MODULE_NAME"
}

# Create smart-module
# Currently just using an empty file
@test "Create smart-module" {
    run timeout 15s "$FLUVIO_BIN" smart-module create "$SMART_MODULE_NAME" --wasm-file "$(mktemp)"
    debug_msg "status: $status"
    echo "cmd: $BATS_RUN_COMMAND"
    debug_msg "output: ${lines[0]}"
    assert_output "smart-module \"$SMART_MODULE_NAME\" has been created."
}

# Create smart-module - Negative test
@test "Attempt to create a smart-module with same name" {
    skip "Smartmodule creation doesn't fail w/ same names at the moment"
    run timeout 15s "$FLUVIO_BIN" smart-module create "$SMART_MODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    echo "cmd: $BATS_RUN_COMMAND"
    assert_failure
    assert_output --partial "Smartmodule already exists"
}

# Describe smart-module
@test "Describe smart-module" {
    skip "Describe is not yet implemented"
    run timeout 15s "$FLUVIO_BIN" smart-module describe "$SMART_MODULE_NAME" 
    debug_msg "status: $status"
    debug_msg "output: $output"
    assert_success
}

# List smart-module
@test "List smart-module" {
    run timeout 15s "$FLUVIO_BIN" smart-module list
    debug_msg "status: $status"
    debug_msg "output: $output"
    assert_success
}

# Delete smart-module
@test "Delete smart-module" {
    run timeout 15s "$FLUVIO_BIN" smart-module delete "$SMART_MODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete smart-module - Negative test
@test "Attempt to delete a smart-module that doesn't exist" {
    run timeout 15s "$FLUVIO_BIN" smart-module delete "$SMART_MODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[3]}"
    assert_failure
    assert_output --partial "Smart Module not found"
}
