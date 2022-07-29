#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    SKIP_CLUSTER_START=1
}

@test "Start K8 with port forwarding" {
    run timeout 15s "$FLUVIO_BIN" cluster start --namespace fluvio-system --use-k8-port-forwarding
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}
