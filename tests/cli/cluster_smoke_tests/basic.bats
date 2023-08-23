#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash


setup_file() {
    "$FLUVIO_BIN" cluster delete
    sleep 15
}

@test "Creates Cluster with \"--spu 0\"" {
    if [[ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]]; then
        skip "don't use --spu 0 on stable version"
    fi

    run timeout 15s "$FLUVIO_BIN" cluster delete || true
    assert_success

    make -C k8-util/helm clean
    sleep 15

    run timeout 15s "$FLUVIO_BIN" cluster start
    assert_success

    sleep 20s
}

@test "Creates Local Cluster with \"--spu 0\"" {
    if [[ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]]; then
        skip "don't use --spu 0 on stable version"
    fi

    run timeout 15s "$FLUVIO_BIN" cluster delete || true
    assert_success

    make -C k8-util/helm clean
    sleep 15

    run timeout 15s "$FLUVIO_BIN" cluster start
    assert_success

    sleep 20s
}
