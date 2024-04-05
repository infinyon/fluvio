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

    EDGE_NAME=edge-test-1
    export EDGE_NAME
    debug_msg "Topic name: $EDGE_NAME"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" cluster shutdown
}

@test "Can register an edge cluster" {
    run timeout 15s "$FLUVIO_BIN" core register "$EDGE_NAME"

    assert_output "edge cluster \"$EDGE_NAME\" was registered"
    assert_success
}

@test "Can't register an edge cluster with the same name" {
    run timeout 15s "$FLUVIO_BIN" core register "$EDGE_NAME"

    assert_output "remote cluster \"$EDGE_NAME\" already exists"
    assert_failure
}

@test "Can unregister an edge cluster" {
    run timeout 15s "$FLUVIO_BIN" core unregister "$EDGE_NAME"

    assert_output "edge cluster \"$EDGE_NAME\" was unregistered"
    assert_success
}

@test "Can't unregister an edge cluster that doesn't exist" {
    run timeout 15s "$FLUVIO_BIN" core unregister "$EDGE_NAME"

    assert_output "remote cluster \"$EDGE_NAME\" not found"
    assert_failure
}
