#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

# Create topic
@test "Export current profile" {
    debug_msg "Export current profile"
    run timeout 15s "$FLUVIO_BIN" profile export
    debug_msg "status: $status"
    debug_msg "output: ${lines[@]}"
    assert_success
    assert_output --partial 'endpoint'
    assert_output --partial 'tls'
}
