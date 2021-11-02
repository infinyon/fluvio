#!/usr/bin/env bats

load "$BATS_TEST_DIRNAME"/../test_helper/tools_check.bash
load "$BATS_TEST_DIRNAME"/../test_helper/fluvio_dev.bash
load "$BATS_TEST_DIRNAME"/../test_helper/bats-support/load.bash
load "$BATS_TEST_DIRNAME"/../test_helper/bats-assert/load.bash

setup_file() {
    SMARTMODULE_NAME=$(random_string)
    export SMARTMODULE_NAME
    debug_msg "SmartModule name: $SMARTMODULE_NAME"
}

# Create smartmodule
# Currently just using an empty file
@test "Create smartmodule" {
    run "$FLUVIO_BIN" smartmodule create "$SMARTMODULE_NAME" --wasm-file "$(mktemp)"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create smartmodule - Negative test
@test "Attempt to create a smartmodule with same name" {
    skip "Smartmodule creation doesn't fail w/ same names at the moment"
    run "$FLUVIO_BIN" smartmodule create "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "Smartmodule already exists"
}

# Describe smartmodule
@test "Describe smartmodule" {
    skip "Describe is not yet implemented"
    run "$FLUVIO_BIN" smartmodule describe "$SMARTMODULE_NAME" 
    debug_msg "status: $status"
    debug_msg "output: $output"
    assert_success
}

# List smartmodule
@test "List smartmodule" {
    skip "List is unstable in CI"
    run "$FLUVIO_BIN" smartmodule list
    debug_msg "status: $status"
    debug_msg "output: $output"
    assert_success
}

# Delete smartmodule
@test "Delete smartmodule" {
    skip "Delete is unstable in CI"
    run "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete smartmodule - Negative test
@test "Attempt to delete a smartmodule that doesn't exist" {
    skip "Delete is unstable in CI"
    run "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[3]}"
    assert_failure
    assert_output --partial "SmartModuleNotFound"
}