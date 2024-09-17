#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    # TOPIC_NAME=$(random_string)
    TOPIC_NAME=test-bsc
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    run rm $TOPIC_NAME.txt
}

# Create topic
@test "Create topics for test" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
}

# regression test issue https://github.com/infinyon/fluvio/issues/4161
# Checking for max batch size ignores compression
@test "Produce uncompressed message larger than batch size" {
    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 2500000 > $TOPIC_NAME-small.txt"
    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 5000001 > $TOPIC_NAME-med.txt"
    # -big gzips from to about 14M to 36k
    run bash -c "yes abcdefghijklmnopqrstuvwxyz |head -c 15000000 > $TOPIC_NAME-big.txt"

    debug_msg small 25
    run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --file $TOPIC_NAME-small.txt --raw --compression gzip'
    assert_success

    debug_msg med 50+1
    run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --file $TOPIC_NAME-med.txt --raw --compression gzip'
    assert_success

    # this will fail if using estimated compression
    # debug_msg big 150
    # run bash -c 'timeout 65s "$FLUVIO_BIN" produce "$TOPIC_NAME" --batch-size 5000000 --file $TOPIC_NAME-big.txt --raw --compression gzip'
    # assert_success
}
