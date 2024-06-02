#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

# Add at least one of each type of resource into the cluster
setup_file() {

    # metadata
    FLUVIO_METADATA_DIR="$HOME/.fluvio/data/metadata"
    export FLUVIO_METADATA_DIR
    debug_msg "Fluvio Metadata Directory: $FLUVIO_METADATA_DIR"

    # topic
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)"
}

# Delete the cluster
@test "Delete the cluster" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "dev" -a "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on stable cluster version and dev cli version" # remove this when installation type is available on stable
    fi

    run bash -c "$FLUVIO_BIN cluster delete --force || $FLUVIO_BIN cluster delete"
    assert_success

    run test -d $FLUVIO_METADATA_DIR
    assert_failure
}

