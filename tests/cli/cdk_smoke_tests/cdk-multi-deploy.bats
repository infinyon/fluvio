#!/usr/bin/env bats

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
    CONFIG2_FILE_FLAG_V2="--config sample-config2-v2.yaml"
    export CONFIG2_FILE_FLAG_V2
}

@test "Build and deploy multiple connectors" {
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

    # Deploy the same config, with a different connector name
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start \
        $CONFIG2_FILE_FLAG_V2
    assert_success
    assert_output --partial "Connector runs with process id"
    sleep 10

    run cat my-json-test-connector.log
    assert_output --partial "producing a value"
    assert_success

    run cat my-json-test-connector2.log
    assert_output --partial "producing a value"
    assert_success

    run $CDK_BIN deploy shutdown --name my-json-test-connector
    assert_success

    run $CDK_BIN deploy shutdown --name my-json-test-connector2
    assert_success
}

@test "Deploy duplicated connectors should fail" {
    # Deploy
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start \
	$CONFIG_FILE_FLAG
    assert_success
    assert_output --partial "Connector runs with process id"

    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start \
	$CONFIG_FILE_FLAG
    assert_failure
    assert_output --partial "Connector with name my-json-test-connector already exists"


    run $CDK_BIN deploy shutdown --name my-json-test-connector
    assert_success
}

@test "Run multiple connectors with --ipkg" {
    # create package meta doesn't exist
    cd $CONNECTOR_DIR
    run $CDK_BIN publish --pack --target x86_64-unknown-linux-gnu
    assert_success

    IPKG_DIR=$TEST_DIR/ipkg_v2

    mkdir $IPKG_DIR
    cp .hub/json-test-connector-0.1.0.ipkg $IPKG_DIR
    cp sample-config-v2.yaml $IPKG_DIR
    cp sample-config2-v2.yaml $IPKG_DIR

    cd $IPKG_DIR

    run $CDK_BIN deploy start --ipkg json-test-connector-0.1.0.ipkg --config sample-config-v2.yaml
    assert_success
    assert_output --partial "Connector runs with process id"

    run $CDK_BIN deploy start --ipkg json-test-connector-0.1.0.ipkg --config sample-config2-v2.yaml
    assert_success
    assert_output --partial "Connector runs with process id"

    run $CDK_BIN deploy shutdown --name my-json-test-connector
    assert_success

    run $CDK_BIN deploy shutdown --name my-json-test-connector2
    assert_success
}
