#!/usr/bin/env bats

SKIP_CLUSTER_START=true
export SKIP_CLUSTER_START

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    PROJECT_NAME_PREFIX="$(random_string)"
    export PROJECT_NAME_PREFIX
    TEST_DIR="$(mktemp -d -t cdk-test.XXXXX)"
    export TEST_DIR

    CONNECTOR_DIR="$(pwd)/connector/json-test-connector"
    export CONNECTOR_DIR

    CONFIG_FILE_FLAG="--config config-example.yaml"
    export CONFIG_FILE_FLAG
}

@test "Build and test connector" {
    # Test
    cd $CONNECTOR_DIR
    run $CDK_BIN test \
        $CONFIG_FILE_FLAG 
    assert_success

    assert_output --partial "Connector runs with process id"
    assert_output --partial "producing a value"
    assert_success
}

@test "Build and deploy connector" {
    # Build
    cd $CONNECTOR_DIR
    run $CDK_BIN build 
    assert_success

    # Deploy
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy start \
        $CONFIG_FILE_FLAG 
    assert_success

    assert_output --partial "Connector runs with process id"

    sleep 10

    run cat json-test-connector.log
    assert_output --partial "producing a value"
    assert_success
}

@test "Pack connector" {
    # Pack when package meta doesn't exist
    cd $CONNECTOR_DIR
    run $CDK_BIN publish --pack 
    assert_success

    # Pack when package meta exists
    cd $CONNECTOR_DIR
    run $CDK_BIN publish --pack 
    assert_success

}
