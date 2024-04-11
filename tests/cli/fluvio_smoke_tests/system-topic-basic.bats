#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash


@test "System topic (Consumer Offsets) exist" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    end_time=$((SECONDS + 65))
    while [ $SECONDS -lt $end_time ]; do
      SYSTEM_TOPIC_NAME="$($FLUVIO_BIN topic list --system -O json | jq '.[0].name' | tr -d '"')"
      if [ -z "$SYSTEM_TOPIC_NAME" ]; then
          debug_msg "$SYSTEM_TOPIC_NAME"
          sleep 1
      else
          debug_msg "System topic $SYSTEM_TOPIC_NAME found"
          break
      fi
    done
    assert [ ! -z "$SYSTEM_TOPIC_NAME" ]
}

# System topic deletion - Negative test
@test "System topic deletion is not allowed by default" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    SYSTEM_TOPIC_NAME="$($FLUVIO_BIN topic list --system -O json | jq '.[0].name' | tr -d '"')"
    run timeout 15s "$FLUVIO_BIN" topic delete "$SYSTEM_TOPIC_NAME"

    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "system topic '$SYSTEM_TOPIC_NAME' can only be deleted forcibly"
}

# System topic deletion - Positive test
@test "System topic deletion is allowed if forced" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    SYSTEM_TOPIC_NAME="$($FLUVIO_BIN topic list --system -O json | jq '.[0].name' | tr -d '"')"
    run timeout 15s "$FLUVIO_BIN" topic delete "$SYSTEM_TOPIC_NAME" --system --force 

    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
    assert_output --partial "system topic \"$SYSTEM_TOPIC_NAME\" deleted"
}

