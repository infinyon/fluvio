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
    # the stable version of cli 0.11.11 or older does not support this type of profile export
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi

    run timeout 15s "$FLUVIO_BIN" profile export
    debug_msg "status: $status"
    debug_msg "output: ${lines[@]}"
    assert_success

    # rerun the export cmd because bats eats the output
    export TMPFILE=$(mktemp -t fluvio_profile_test.XXXXXX)
    debug_msg "Export current profile to $TMPFILE"
    "$FLUVIO_BIN" profile export > $TMPFILE
    export FLV_PROFILE_PATH=$TMPFILE
    run timeout 15s "$FLUVIO_BIN" topic list
    EXP_PROFILE=$(cat $TMPFILE)
    debug_msg "# FLV_PROFILE_PATH: ${FLV_PROFILE_PATH}"
    debug_msg "# exported profile:\n$(cat $TMPFILE)"
    assert_success
}

