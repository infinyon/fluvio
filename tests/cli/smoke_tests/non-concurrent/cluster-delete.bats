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
    run "$FLUVIO_BIN" cluster delete
    assert_success
}

# The rest will be validated by `kubectl`
@test "No connector pods left in K8 cluster" {
    # CI is kind of slow to terminate the pod, so we just care that no connector pods are running
    run kubectl get po -l app=fluvio-connector --field-selector=status.phase==Running
    # `kubectl` will still return pods that are in Terminating state
    refute_output --partial 'Running'
}

# CRD Resource Deletion checks
#

@test "Connectors deleted" {
    run kubectl get managedconnectors
    assert_output 'error: the server doesn'\''t have a resource type "managedconnectors"'
}

@test "SPU Groups deleted" {
    run kubectl get spugroups 
    assert_output 'error: the server doesn'\''t have a resource type "spugroups"'
}

@test "Topics deleted" {
    run kubectl get topics 
    assert_output 'error: the server doesn'\''t have a resource type "topics"'
}

@test "SmartModules deleted" {
    run kubectl get smartmodules
    assert_output 'error: the server doesn'\''t have a resource type "smartmodules"'
}

@test "Partitions deleted" {
    run kubectl get partitions 
    assert_output 'error: the server doesn'\''t have a resource type "partitions"'
}

@test "DerivedStreams deleted" {
    run kubectl get derivedstreams 
    assert_output 'error: the server doesn'\''t have a resource type "derivedstreams"'
}

@test "SPUs deleted" {
    run kubectl get spus 
    assert_output 'error: the server doesn'\''t have a resource type "spus"'
}

@test "TableFormats deleted" {
    run kubectl get tableformats
    assert_output 'error: the server doesn'\''t have a resource type "tableformats"'
}
