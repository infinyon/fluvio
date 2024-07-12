#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

# This test needs 2 SPUs running
setup_file() {
    TOPIC_NAME=$(random_string)
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"
}

# Create topic
@test "Create a topic" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
}

# Add partitions to topic
@test "Add 2 new partitions to the topic" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    debug_msg "Add partitions to topic"
    run timeout 15s "$FLUVIO_BIN" topic add-partition "$TOPIC_NAME" -c 2

    SPU_OF_PARTION1=$(echo "${lines[2]}" | awk '{print $2}')
    SPU_OF_PARTION2=$(echo "${lines[3]}" | awk '{print $2}')

    assert_line --index 0 "added new partitions to topic: "\"$TOPIC_NAME\"""
    assert_line --partial --index 1 "PARTITION  SPU"
    assert_line --partial --index 2 "1          $SPU_OF_PARTION1"
    assert_line --partial --index 3 "2          $SPU_OF_PARTION2"

    assert_success
}

# List partitions
@test "List partitions should have 3 partitions" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    sleep 2
    debug_msg "List partitions"
    run timeout 15s "$FLUVIO_BIN" partition list

    #TODO: filter without grep when fluvio partition list has a filter option
    run bash -c 'timeout 15s "$FLUVIO_BIN" partition list | grep "$TOPIC_NAME"'
    assert_success
    assert_line --partial --index 0 "$TOPIC_NAME  0          500"
    assert_line --partial --index 1 "$TOPIC_NAME  1          $SPU_OF_PARTION1"
    assert_line --partial --index 2 "$TOPIC_NAME  2          $SPU_OF_PARTION2"
    assert [ ${#lines[@]} -eq 3 ]
}

# Not found topic
@test "Add partitions to a topic that doesn't exist" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    debug_msg "Add partitions to a topic that doesn't exist"
    run timeout 15s "$FLUVIO_BIN" topic add-partition "not-exist-topic" -c 2
    assert_output --partial 'topic "not-exist-topic" not found'
    assert_failure
}

# Produce must recognize new partitions
@test "Produce message to new partitions while connected" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi

    debug_msg "Produce message to actual partitions, then wait the new one and produce to it"
    run bash -c '/usr/bin/expect << EOF
    spawn "$FLUVIO_BIN" produce "$TOPIC_NAME"
    expect "> "
    send "1\r"
    expect "Ok!"
    expect "> "
    send "2\r"
    expect "Ok!"
    expect "> "
    send "3\r"
    expect "Ok!"
    expect "> "
    exec "$FLUVIO_BIN" topic add-partition "$TOPIC_NAME"
    sleep 2
    send "4\r"
    expect "Ok!"
    expect "> "
    send "5\r"
    expect "Ok!"
    expect "> "
    exit
    EOF'
    assert_success

    sleep 2
    debug_msg "Check if the new partition received the message"
    run bash -c 'timeout 15s "$FLUVIO_BIN" partition list | grep "$TOPIC_NAME"'
    assert_success
    assert_line --partial --index 0 "2   2    2    0"
    assert_line --partial --index 1 "1   1    1    0"
    assert_line --partial --index 2 "1   1    1    0"
    assert_line --partial --index 3 "1   1    1    0"
    assert [ ${#lines[@]} -eq 4 ]
}

# Delete topic
@test "Delete topic must delete all partitions" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi

    debug_msg "Delete topic"
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_success

    sleep 1
    debug_msg "Check if the new partition received the message"
    run bash -c 'timeout 15s "$FLUVIO_BIN" partition list | grep "$TOPIC_NAME"'
    assert [ ${#lines[@]} -eq 0 ]
}
