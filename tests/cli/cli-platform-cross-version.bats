#!/usr/bin/env bats

SKIP_CLUSTER_START=true
export SKIP_CLUSTER_START

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    CLUSTER_VERSION="${CLUSTER_VERSION:-latest}"
    export CLUSTER_VERSION

    CLI_VERSION="${CLI_VERSION:-latest}"
    export CLI_VERSION

    PAYLOAD_SIZE="${PAYLOAD_SIZE:-100}"
    export PAYLOAD_SIZE

    TOPIC_NAME="$(echo $CLI_VERSION-x-$CLUSTER_VERSION | tr '.' '-')"
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"

    MESSAGE="$(random_string "$PAYLOAD_SIZE")"
    export MESSAGE
    debug_msg "$MESSAGE"

    CI="${CI:-}"
    SKIP_SETUP="${SKIP_SETUP:-}"
    SKIP_CLEANUP="${SKIP_CLEANUP:-}"

    # If the CI env var isn't set (like when running locally)
    if [[ -z "$CI" ]];
    then
        echo "# Deleting cluster" >&3
        $FLUVIO_BIN cluster delete
    else
        echo "# [CI MODE] Skipping initial cleanup" >&3
    fi;

    # By default, set up the cluster and cli before running test
    # set SKIP_SETUP to skip
    if [[ -z "$SKIP_SETUP" ]];
    then
        setup_fluvio_cluster "$CLUSTER_VERSION";
        setup_fluvio_cli "$CLI_VERSION";
    else
        echo "# Skipping setup" >&3
    fi;

}

teardown_file() {
    # By default, delete the cluster at the end of the test
    # set SKIP_CLEANUP to skip
    if [[ -z "$SKIP_CLEANUP" ]];
    then
        echo "# Deleting cluster" >&3
        "$FLUVIO_BIN" cluster delete
    else
        echo "# Skipping cleanup" >&3
    fi

    #run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
}

# Create topic
@test "Create a topic: $TOPIC_NAME" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
}

# Produce message 
@test "Produce message" {
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'

    assert_success
}

# Consume message and compare message
# Warning: Adding anything extra to the `debug_msg` skews the message comparison
@test "Consume message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d

    assert_output --partial "$MESSAGE"
    assert_success
}
