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
    # connector
    CONNECTOR_CONFIG="$TEST_HELPER_DIR/test-connector-config.yml"
    export CONNECTOR_CONFIG
    run timeout 15s "$FLUVIO_BIN" smart-module create "$(random_string)" --wasm-file "$(mktemp)"
    run timeout 15s "$FLUVIO_BIN" connector create --config "$CONNECTOR_CONFIG"
    # smart-module
    run timeout 15s "$FLUVIO_BIN" smart-module create "$(random_string)" --wasm-file "$(mktemp)"
    # table-format
    TABLE_FORMAT_CONFIG="$TEST_HELPER_DIR/test-table-format-config.yml"
    export TABLE_FORMAT_CONFIG
    run timeout 15s "$FLUVIO_BIN" table-format create --config "$TABLE_FORMAT_CONFIG"
    # TODO: derived-streams
}

# Delete the cluster 
@test "Delete the cluster" {
    run timeout 15s "$FLUVIO_BIN" cluster delete
    assert_success
}

# The rest will be validated by `kubectl`
@test "No connector pods left in K8 cluster" {
    # Do the pods have a label?
    run kubectl get pods -l app=fluvio-connector
    assert_output 'No resources found in default namespace.'
}

# CRD Resource Deletion checks
#

@test "Connectors deleted" {
    run kubectl get managedconnectors
    assert_output 'No resources found in default namespace.'
}

@test "SPU Groups deleted" {
    run kubectl get spugroups 
    assert_output 'No resources found in default namespace.'
}

@test "Topics deleted" {
    run kubectl get topics 
    assert_output 'No resources found in default namespace.'
}

@test "SmartModules deleted" {
    run kubectl get smartmodules
    assert_output 'No resources found in default namespace.'
}

@test "Partitions deleted" {
    run kubectl get partitions 
    assert_output 'No resources found in default namespace.'
}

@test "DerivedStreams deleted" {
    run kubectl get derivedstreams 
    assert_output 'No resources found in default namespace.'
}

@test "SPUs deleted" {
    run kubectl get spus 
    assert_output 'No resources found in default namespace.'
}

@test "TableFormats deleted" {
    skip "table-format deletion isn't working: https://github.com/infinyon/fluvio/issues/2004"
    run kubectl get tableformats
    assert_output 'No resources found in default namespace.'
}
