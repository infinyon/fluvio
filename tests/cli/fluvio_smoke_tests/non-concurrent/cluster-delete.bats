#!/usr/bin/env bats

SKIP_CLUSTER_START=true
export SKIP_CLUSTER_START

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

# Add at least one of each type of resource into the cluster
setup_file() {
  
    FLUVIO_METADATA_DIR="$HOME/.fluvio/data/metadata"
    export FLUVIO_METADATA_DIR
    debug_msg "Fluvio Metadata Directory: $FLUVIO_METADATA_DIR"

    # topic
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)"
    run timeout 15s "$FLUVIO_BIN" smartmodule create "$(random_string)" --wasm-file "$(mktemp)"
    # smartmodule
    run timeout 15s "$FLUVIO_BIN" smartmodule create "$(random_string)" --wasm-file "$(mktemp)"
    # table-format
    TABLE_FORMAT_CONFIG="$TEST_HELPER_DIR/test-table-format-config.yml"
    export TABLE_FORMAT_CONFIG
    run timeout 15s "$FLUVIO_BIN" table-format create --config "$TABLE_FORMAT_CONFIG"
}

# Delete the cluster
@test "Delete the local cluster" {
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    run "$FLUVIO_BIN" cluster delete --local
    assert_success
}


@test "Local metadata deleted" {
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    run test -d $FLUVIO_METADATA_DIR
    assert_failure
}

@test "Delete the cluster" {
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "dev" ]; then
        skip "don't run on cluster dev version"
    fi
    run "$FLUVIO_BIN" cluster delete
    assert_success
}

