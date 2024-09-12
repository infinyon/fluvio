#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash


@test "Export current profile json" {
    debug_msg "Export current profile"
    run timeout 15s "$FLUVIO_BIN" profile export -O json
    debug_msg "status: $status"
    debug_msg "output: ${lines[@]}"
    assert_success
    assert_output --partial 'endpoint'
    assert_output --partial 'tls'
}

@test "Export current profile for use with FLV_PROFILE_PATH" {
    TMPFILE=$(mktemp -t fluvio_profile_test.XXXXXX)
    debug_msg "Export current profile"
    run timeout 15s "$FLUVIO_BIN" profile export > $TMPFILE
    debug_msg "status: $status"
    debug_msg "output: ${lines[@]}"
    assert_success

    export FLV_PROFILE_PATH=$TMPFILE
    run timeout 15s "$FLUVIO_BIN" topic list
    assert_success
}

