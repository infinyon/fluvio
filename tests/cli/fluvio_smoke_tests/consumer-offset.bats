#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    TOPIC_NAME=$(random_string)
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" --partitions 2

    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 2:2 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 4:4 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 8:8 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
}

@test "Consumer Offsets topic exist" {
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

@test "Read one partition with consumer" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi

    CONSUMER_NAME=$(random_string)
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --consumer $CONSUMER_NAME -p 1 -B -d
    assert_success
    assert_line "4"
    assert_line "8"

    OFFSET=$("$FLUVIO_BIN" consumer list -O json | jq ".[] | select(.consumer_id == \"$CONSUMER_NAME\") | .offset")
    assert [ $OFFSET == "1" ]

    run timeout 15s "$FLUVIO_BIN" consumer delete "$CONSUMER_NAME"
    assert_output --partial "consumer \"$CONSUMER_NAME\" on topic \"$TOPIC_NAME\" and partition \"1\" deleted"
}

@test "Read many partitions with consumer" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi

    CONSUMER_NAME=$(random_string)
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --consumer $CONSUMER_NAME -B -d
    assert_success
    assert_line "1"
    assert_line "2"
    assert_line "4"
    assert_line "8"

    OFFSET1=$("$FLUVIO_BIN" consumer list -O json | jq ".[] | select(.consumer_id == \"$CONSUMER_NAME\" and .partition == 0) | .offset")
    assert [ $OFFSET1 == "1" ]
    OFFSET2=$("$FLUVIO_BIN" consumer list -O json | jq ".[] | select(.consumer_id == \"$CONSUMER_NAME\" and .partition == 1) | .offset")
    assert [ $OFFSET2 == "1" ]

    run timeout 15s "$FLUVIO_BIN" consumer delete "$CONSUMER_NAME"
    assert_output --partial "consumer \"$CONSUMER_NAME\" on topic \"$TOPIC_NAME\" and partition \"0\" deleted"
    assert_output --partial "consumer \"$CONSUMER_NAME\" on topic \"$TOPIC_NAME\" and partition \"1\" deleted"
}

@test "Delete topic must delete consumers" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi

    CONSUMER_NAME=$(random_string)
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --consumer "$CONSUMER_NAME" -B -d
    assert_success

    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_output --partial "topic \"$TOPIC_NAME\" deleted"

    run timeout 15s "$FLUVIO_BIN" consumer list
    assert_output --partial "No consumers found"
}

# This is a regression test for issue #4455
# Check that is not using the old consumer offset with a higher start(4)
# to a new topic that doesn't even have 4 records.
@test "Delete topic must delete consumers and allow to create the same topic and consumer again" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
	skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
	skip "don't run on cluster stable version"
    fi

    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" -p 2 -r 2
    assert_output --partial "topic \"$TOPIC_NAME\" created"

    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 2:2 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 2:2 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"

    CONSUMER_NAME=$(random_string)
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --consumer "$CONSUMER_NAME" -B -d
    assert_success

    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_output --partial "topic \"$TOPIC_NAME\" deleted"

    run timeout 15s "$FLUVIO_BIN" consumer list
    assert_output --partial "No consumers found"

    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" -p 2 -r 2
    assert_output --partial "topic \"$TOPIC_NAME\" created"

    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 2:2 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"

    CONSUMER_NAME=$(random_string)
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --consumer "$CONSUMER_NAME" -B -d
    assert_success # this should not panic

    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_output --partial "topic \"$TOPIC_NAME\" deleted"
    sleep 1
}

@test "End offset with auto as strategy should commit and flush the offset" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
	skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
	skip "don't run on cluster stable version"
    fi

    CONSUMER_NAME=$(random_string)
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_output --partial "topic \"$TOPIC_NAME\" created"

    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 2:2 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"

    # easy wat to create a consumer is with start offset
    CONSUMER_NAME=$(random_string)
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --consumer "$CONSUMER_NAME" -B -d
    assert_success
    assert_line "1"
    assert_line "2"

    echo 3:3 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 4:4 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"

    # consumer with end offset
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --consumer "$CONSUMER_NAME" -d
    assert_success
    assert_line "3"
    assert_line "4"

    OFFSET=$("$FLUVIO_BIN" consumer list -O json | jq ".[] | select(.consumer_id == \"$CONSUMER_NAME\") | .offset")
    assert [ $OFFSET == "3" ]

    # cleanup
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_output --partial "topic \"$TOPIC_NAME\" deleted"
}

