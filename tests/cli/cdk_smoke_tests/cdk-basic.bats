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

    CONFIG_FILE_FLAG="--config sample-config.yaml"
    export CONFIG_FILE_FLAG
    CONFIG_FILE_FLAG_V2="--config sample-config-v2.yaml"
    export CONFIG_FILE_FLAG_V2
}

@test "Build and test connector" {
    # Test
    cd $CONNECTOR_DIR
    run $CDK_BIN test --target x86_64-unknown-linux-gnu \
        $CONFIG_FILE_FLAG 
    assert_success

    assert_output --partial "Connector runs with process id"
    assert_output --partial "producing a value"
    assert_success
}

@test "Build and test connector V2" {
    # Test
    cd $CONNECTOR_DIR
    run $CDK_BIN test --target x86_64-unknown-linux-gnu \
        $CONFIG_FILE_FLAG_V2 
    assert_success

    assert_output --partial "Connector runs with process id"
    assert_output --partial "producing a value"
    assert_success
}

@test "Build and deploy connector" {
    # Build
    cd $CONNECTOR_DIR
    run $CDK_BIN build --target x86_64-unknown-linux-gnu
    assert_success

    # Deploy
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start \
        $CONFIG_FILE_FLAG 
    assert_success

    assert_output --partial "Connector runs with process id"

    sleep 10

    run cat json-test-connector.log
    assert_output --partial "producing a value"
    assert_success
}

@test "Build and deploy connector V2" {
    # Build
    cd $CONNECTOR_DIR
    run $CDK_BIN build --target x86_64-unknown-linux-gnu
    assert_success

    # Deploy
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start \
        $CONFIG_FILE_FLAG_V2 
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
    run $CDK_BIN publish --pack --target x86_64-unknown-linux-gnu
    assert_success

    # Pack when package meta exists
    cd $CONNECTOR_DIR
    run $CDK_BIN publish --pack --target x86_64-unknown-linux-gnu
    assert_success
}

@test "Packs connector with specific README.md" {
    # Creates a directory to store the dummy readme
    cd $CONNECTOR_DIR

    mkdir ../testing
    echo "# Testing Connector Readme" > ../testing/README.md

    run $CDK_BIN publish --pack --target x86_64-unknown-linux-gnu --readme ../testing/README.md
    assert_success

    # Ensure the correct path is added
    cat ./.hub/package-meta.yaml | grep '../../testing/README.md'
    assert_success

    # clean up
    rm -r ../testing
}

@test "Run connector with --ipkg" {
    # create package meta doesn't exist
    cd $CONNECTOR_DIR
    run $CDK_BIN publish --pack --target x86_64-unknown-linux-gnu
    assert_success

    IPKG_DIR=$TEST_DIR/ipkg

    mkdir $IPKG_DIR
    cp .hub/json-test-connector-0.1.0.ipkg $IPKG_DIR
    cp sample-config.yaml $IPKG_DIR 

    cd $IPKG_DIR

    run $CDK_BIN deploy start --ipkg json-test-connector-0.1.0.ipkg --config sample-config.yaml
    assert_success
    assert_output --partial "Connector runs with process id"
}

@test "Run connector with --ipkg V2" {
    # create package meta doesn't exist
    cd $CONNECTOR_DIR
    run $CDK_BIN publish --pack --target x86_64-unknown-linux-gnu
    assert_success

    IPKG_DIR=$TEST_DIR/ipkg_v2

    mkdir $IPKG_DIR
    cp .hub/json-test-connector-0.1.0.ipkg $IPKG_DIR
    cp sample-config-v2.yaml $IPKG_DIR 

    cd $IPKG_DIR

    run $CDK_BIN deploy start --ipkg json-test-connector-0.1.0.ipkg --config sample-config-v2.yaml
    assert_success
    assert_output --partial "Connector runs with process id"
}

# fix CI authentication to hub service first:
# https://github.com/infinyon/fluvio/issues/3634
# @test "List connectors from hub" {
#     run timeout 15s $CDK_BIN hub list
#     assert_success
# }
