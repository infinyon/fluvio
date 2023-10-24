#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup() {
    # * PROFILE   CLUSTER   [ADDRESS]             TLS
    LOCAL_ADDRESS=$($FLUVIO_BIN profile list | grep '*' | awk '{print $4}')
    # create fake `cloud` profile that points to local cluster address
    $FLUVIO_BIN profile add "cloud" $LOCAL_ADDRESS
}

@test "local profile can run cluster command" {
    run $FLUVIO_BIN profile switch local
    refute_output "profile local not found"
    run $FLUVIO_BIN profile
    assert_output "local"

    run $FLUVIO_BIN cluster status
    assert_success
}

@test "cloud profile cannot run cluster command" {
    run $FLUVIO_BIN profile switch cloud
    assert_success

    run $FLUVIO_BIN cluster spu list
    assert_output --partial "Invalid command on \`cloud\` profile."
}

@test "cloud profile can run \`cluster start\` command" {
    run $FLUVIO_BIN profile switch cloud
    assert_success

    run timeout 1s $FLUVIO_BIN cluster start
    refute_output --partial "Invalid command"
}
