#!/usr/bin/env bats

load test_helper/fluvio_dev.bash
load test_helper/tools_check.bash
load test_helper/setup_k8_cluster.bash
load test_helper/random_string.bash

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
    [ "$status" -eq 0 ]
}

# Create smartmodule - Negative test
@test "Attempt to create a smartmodule with same name" {
    skip "Smartmodule creation doesn't fail w/ same names at the moment"
    run "$FLUVIO_BIN" smartmodule create "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 1 ]
    [ "${lines[0]}" = "Smartmodule already exists" ]
}

# Describe smartmodule
@test "Describe smartmodule" {
    skip "Describe is not yet implemented"
    run "$FLUVIO_BIN" smartmodule describe "$SMARTMODULE_NAME" 
    debug_msg "status: $status"
    debug_msg "output: $output"
    [ "$status" -eq 0 ]
}

# List smartmodule
@test "List smartmodule" {
    run "$FLUVIO_BIN" smartmodule list
    debug_msg "status: $status"
    debug_msg "output: $output"
    [ "$status" -eq 0 ]
}

# Delete smartmodule
@test "Delete smartmodule" {
    run "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    [ "$status" -eq 0 ]
}

# Delete smartmodule - Negative test
@test "Attempt to delete a smartmodule that doesn't exist" {
    run "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[3]}"
    [ "$status" -eq 1 ]
    [[ "${lines[3]}" =~ 'SmartModuleNotFound' ]]
}