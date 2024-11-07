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

    REMOTE_NAME_2="$(random_string 7)"
    export REMOTE_NAME_2
    debug_msg "Remote name 2: $REMOTE_NAME_2"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"

    TOPIC_NAME="$(random_string 7)"
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"
}

@test "Can register an remote clusters" {
    run timeout 15s "$FLUVIO_BIN" remote register "$REMOTE_NAME"

    assert_output "remote cluster \"$REMOTE_NAME\" was registered"
    assert_success

    run timeout 15s "$FLUVIO_BIN" remote register "$REMOTE_NAME_2"

    assert_output "remote cluster \"$REMOTE_NAME_2\" was registered"
    assert_success
}

@test "Can create a mirror topic" {
    echo "[\"$REMOTE_NAME\"]" > remotes.json
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" --mirror-apply remotes.json --home-to-remote

    assert_output "topic \"$TOPIC_NAME\" created"
    assert_success
}

@test "Can add a new remote to the mirror topic" {
    run timeout 15s "$FLUVIO_BIN" topic add-mirror "$TOPIC_NAME" "$REMOTE_NAME_2" --home-to-remote

    assert_output "added new mirror: \"$REMOTE_NAME_2\" to topic: \"$TOPIC_NAME\""
    assert_success
}

@test "Can't add a non existent remote to the mirror topic" {
    run timeout 15s "$FLUVIO_BIN" topic add-mirror "$TOPIC_NAME" "nonexistent-remote" --home-to-remote

    assert_output "Mirror not found"
    assert_failure
}

@test "Can't add a remote to the mirror topic that doesn't exist" {
    run timeout 15s "$FLUVIO_BIN" topic add-mirror "nonexistent-topic" "$REMOTE_NAME" --home-to-remote

    assert_output "Topic not found"
    assert_failure
}

@test "Can't add a remote to the mirror topic that is already assigned" {
    run timeout 15s "$FLUVIO_BIN" topic add-mirror "$TOPIC_NAME" "$REMOTE_NAME" --home-to-remote

    assert_output "remote \"$REMOTE_NAME\" is already assigned to partition: \"0\"" 
    assert_failure
}


@test "List topics" {
    run bash -c 'timeout 15s "$FLUVIO_BIN" topic list | grep "$TOPIC_NAME"'
    assert_success
    assert_line --partial --index 0 "$TOPIC_NAME  to-remote"
}

@test "List partitions" {
    run bash -c 'timeout 15s "$FLUVIO_BIN" partition list | grep "$TOPIC_NAME"'
    assert_success
    assert_line --partial --index 0 "$TOPIC_NAME  0          5001    $REMOTE_NAME(to-remote)"
}
