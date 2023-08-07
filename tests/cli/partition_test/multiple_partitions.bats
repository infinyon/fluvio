#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME=$(random_string)
    export PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME
    debug_msg "Topic name: $PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME"

    MULTI_LINE_FILE_NAME=$(random_string)
    export MULTI_LINE_FILE_NAME

    for i in {1..4}
    do
        echo $i >> "$MULTI_LINE_FILE_NAME"
    done
}

teardown_file() {
    echo "Tearing down, shutting down cluster components"
    "$FLUVIO_BIN" topic delete "$PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME"
}

@test "Create a topic for P/C Multiple Partitions" {
    echo "Creates Topic: $PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME for P/C Multiple Partitions"
    run timeout 15s "$FLUVIO_BIN" topic create "$PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME" --partitions 2 --replication 2
    assert_success

    echo "Topic Details: $PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic describe "$PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME"
    assert_success
}

@test "Produces on topic for P/C Multiple Partitions" {
    run bash -c 'timeout 15s "$FLUVIO_BIN" produce --file "$MULTI_LINE_FILE_NAME" "$PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME"'
    assert_success
}

@test "Consumes on topic for P/C Multiple Partitions with Partition 0" {
    run timeout 15s "$FLUVIO_BIN" consume "$PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME" -p 0 -B -d
    assert_line --index 0 "1"
    assert_line --index 1 "3"
}

@test "Consumes on topic for P/C Multiple Partitions with Partition 1" {
    run timeout 15s "$FLUVIO_BIN" consume "$PRODUCE_CONSUME_MULTIPLE_PARTITIONS_TOPIC_NAME" -p 1 -B -d
    assert_line --index 0 "2"
    assert_line --index 1 "4"
}
