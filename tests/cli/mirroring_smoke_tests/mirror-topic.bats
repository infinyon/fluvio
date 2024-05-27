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

    REMOTE_NAME="$(random_string 7)"
    export REMOTE_NAME
    debug_msg "Remote name: $REMOTE_NAME"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"

    TOPIC_NAME="$(random_string 7)"
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"
}

@test "Can register an remote cluster" {
    run timeout 15s "$FLUVIO_BIN" remote register "$REMOTE_NAME"

    assert_output "remote cluster \"$REMOTE_NAME\" was registered"
    assert_success
}

@test "Can create a mirror topic" {
    echo "[\"$REMOTE_NAME\"]" > remotes.json
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" --mirror-apply remotes.json

    assert_output "topic \"$TOPIC_NAME\" created"
    assert_success
}

@test "Can't produce to a mirror topic from home" {
    MESSAGE="$(random_string 7)"
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'

    assert_output "cannot produce to mirror topic from home"
    assert_failure
}
