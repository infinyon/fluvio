#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    CURRENT_DATE=$(date +%Y-%m)
    export CURRENT_DATE

    REMOTE_NAME=remote-test-1
    export REMOTE_NAME
    debug_msg "Remote name: $REMOTE_NAME"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"
}

@test "Can register an remote cluster" {
    run timeout 15s "$FLUVIO_BIN" remote register "$REMOTE_NAME"

    assert_output "remote cluster \"$REMOTE_NAME\" was registered"
    assert_success
}

@test "Can't register an remote cluster with the same name" {
    run timeout 15s "$FLUVIO_BIN" remote register "$REMOTE_NAME"

    assert_output "remote cluster \"$REMOTE_NAME\" already exists"
    assert_failure
}

@test "Can unregister an remote cluster" {
    run timeout 15s "$FLUVIO_BIN" remote unregister "$REMOTE_NAME"

    assert_output "remote cluster \"$REMOTE_NAME\" was unregistered"
    assert_success
}

@test "Can't unregister an remote cluster that doesn't exist" {
    sleep 1
    run timeout 15s "$FLUVIO_BIN" remote unregister "$REMOTE_NAME"

    assert_output "Mirror not found"
    assert_failure
}
