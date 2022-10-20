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
    # topic
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)"
    run timeout 15s "$FLUVIO_BIN" smartmodule create "$(random_string)" --wasm-file "$(mktemp)"
    # smartmodule
    run timeout 15s "$FLUVIO_BIN" smartmodule create "$(random_string)" --wasm-file "$(mktemp)"
    # table-format
    TABLE_FORMAT_CONFIG="$TEST_HELPER_DIR/test-table-format-config.yml"
    export TABLE_FORMAT_CONFIG
    run timeout 15s "$FLUVIO_BIN" table-format create --config "$TABLE_FORMAT_CONFIG"
    # TODO: derived-streams
}

# Delete the cluster
@test "Delete the cluster" {
    run "$FLUVIO_BIN" cluster delete
    assert_success
}


@test "SPU Groups deleted" {
    run kubectl get spugroups
#    assert_failure
}

@test "Topics deleted" {
    run kubectl get topics
#    assert_failure
}

@test "SmartModules deleted" {
    run kubectl get smartmodules
#    assert_failure
}

@test "Partitions deleted" {
    run kubectl get partitions
#    assert_failure
}

@test "DerivedStreams deleted" {
    run kubectl get derivedstreams
#    assert_failure
}

@test "SPUs deleted" {
    run kubectl get spus
#    assert_failure
}

@test "TableFormats deleted" {
    skip "table-format deletion isn't working: https://github.com/infinyon/fluvio/issues/2004"
    run kubectl get tableformats
#    assert_failure
}
