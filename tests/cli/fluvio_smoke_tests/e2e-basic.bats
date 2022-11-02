#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    CURRENT_DATE=$(date +%Y-%m)
    export CURRENT_DATE

    TOPIC_NAME=$(random_string)
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"

    TOPIC_NAME_2=$(random_string)
    export TOPIC_NAME_2
    debug_msg "Topic name: $TOPIC_NAME_2"

    TOPIC_NAME_3=$(random_string)
    export TOPIC_NAME_3
    debug_msg "Topic name: $TOPIC_NAME_3"

    TOPIC_NAME_4=$(random_string)
    export TOPIC_NAME_4
    debug_msg "Topic name: $TOPIC_NAME_4"

    TOPIC_NAME_5=$(random_string)
    export TOPIC_NAME_5
    debug_msg "Topic name: $TOPIC_NAME_5"

    TOPIC_NAME_6=$(random_string)
    export TOPIC_NAME_6
    debug_msg "Topic name: $TOPIC_NAME_6"

    TOPIC_NAME_7=$(random_string)
    export TOPIC_NAME_7
    debug_msg "Topic name: $TOPIC_NAME_7"

    TOPIC_NAME_8=$(random_string)
    export TOPIC_NAME_8
    debug_msg "Topic name: $TOPIC_NAME_8"

    TOPIC_NAME_9=$(random_string)
    export TOPIC_NAME_9
    debug_msg "Topic name: $TOPIC_NAME_9"

    TOPIC_NAME_10=$(random_string)
    export TOPIC_NAME_10
    debug_msg "Topic name: $TOPIC_NAME_10"

    TOPIC_NAME_11=$(random_string)
    export TOPIC_NAME_11
    debug_msg "Topic name: $TOPIC_NAME_11"

    TOPIC_NAME_12=$(random_string)
    export TOPIC_NAME_12
    debug_msg "Topic name: $TOPIC_NAME_12"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"

    MESSAGE_W_HTML_STR='"&"'
    export MESSAGE_W_HTML_STR

    MULTILINE_MESSAGE="$MESSAGE\n$MESSAGE_W_HTML_STR"
    export MULTILINE_MESSAGE

    GZIP_MESSAGE="$MESSAGE-GZIP"
    export GZIP_MESSAGE

    SNAPPY_MESSAGE="$MESSAGE-SNAPPY"
    export SNAPPY_MESSAGE

    LZ4_MESSAGE="$MESSAGE-LZ4"
    export LZ4_MESSAGE

    LINGER_MESSAGE="$MESSAGE-LINGER"
    export LINGER_MESSAGE

    BATCH_MESSAGE="$MESSAGE-BATCH_MESSAGE"
    export BATCH_MESSAGE

    READ_COMMITTED_MESSAGE="$MESSAGE-READ_COMMITTED_MESSAGE"
    export READ_COMMITTED_MESSAGE

    READ_UNCOMMITTED_MESSAGE="$MESSAGE-READ_UNCOMMITTED_MESSAGE"
    export READ_UNCOMMITTED_MESSAGE

    AT_MOST_ONCE_MESSAGE="$MESSAGE-AT_MOST_ONCE_MESSAGE"
    export AT_MOST_ONCE_MESSAGE

    AT_LEAST_ONCE_MESSAGE="$MESSAGE-AT_LEAST_ONCE_MESSAGE"
    export AT_LEAST_ONCE_MESSAGE
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
}

# Create topic
@test "Create a topic" {
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_2" --compression-type snappy 
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_3" --compression-type lz4
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_4" --compression-type gzip
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_5"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_6" --compression-type any
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_7"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_8"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_9"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_10"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_11"
    assert_success
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_12"
    assert_success
}

# Produce message 
@test "Produce message" {
    run bash -c 'echo "$MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
    run bash -c 'echo "$MESSAGE_W_HTML_STR" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_2"'
    assert_success
    run bash -c 'echo -e "$MULTILINE_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_3"'
    assert_success
    run bash -c 'echo -e "$GZIP_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_4" --compression gzip'
    assert_success
    run bash -c 'echo -e "$SNAPPY_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_5" --compression snappy'
    assert_success
    run bash -c 'echo -e "$LZ4_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_6" --compression lz4'
    assert_success
    run bash -c 'echo -e "$LINGER_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_7" --linger 0s'
    assert_success
    run bash -c 'echo -e "$BATCH_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_8" --batch-size 100'
    assert_success
    run bash -c 'echo -e "$READ_COMMITTED_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_9" --isolation read_committed'
    assert_success
    run bash -c 'echo -e "$READ_UNCOMMITTED_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_10" --isolation ReadUncommitted'
    assert_success
    run bash -c 'echo -e "$AT_MOST_ONCE_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_11" --delivery-semantic AtMostOnce'
    assert_success
    run bash -c 'echo -e "$AT_LEAST_ONCE_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME_12" --delivery-semantic AtLeastOnce'
    assert_success
}

# Consume message and compare message
# Warning: Adding anything extra to the `debug_msg` skews the message comparison
@test "Consume message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d

    assert_output --partial "$MESSAGE"
    assert_success
}

# Validate that using format doesn't introduce HTML escaping
# https://github.com/infinyon/fluvio/issues/1628
@test "Consume message using format" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_2" --format "{{value}}" -B -d
    assert_output "$MESSAGE_W_HTML_STR"
    assert_success
}

@test "Consume message display timestamp using format" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_2" --format "{{time}}" -B -d
    assert_output --partial "$CURRENT_DATE"
    assert_success
}


# Validate that consume --tail 1, returns only the last record
@test "Consume with tail" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_3" --tail 1 -d
    assert_output "$MESSAGE_W_HTML_STR"
    assert_success
}

@test "Consume gzip message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_4" -B -d

    assert_output --partial "$GZIP_MESSAGE"
    assert_success
}

@test "Consume snappy message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_5" -B -d

    assert_output --partial "$SNAPPY_MESSAGE"
    assert_success
}

@test "Consume lz4 message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_6" -B -d

    assert_output --partial "$LZ4_MESSAGE"
    assert_success
}

@test "ReadCommitted Consume ReadCommitted message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_9" -B -d --isolation read_committed

    assert_output --partial "$READ_COMMITTED_MESSAGE"
    assert_success
}

@test "ReadUncommitted Consume ReadCommitted message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_9" -B -d --isolation read_uncommitted

    assert_output --partial "$READ_COMMITTED_MESSAGE"
    assert_success
}

@test "ReadCommitted Consume ReadUncommitted message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_10" -B -d --isolation ReadCommitted

    assert_output --partial "$READ_UNCOMMITTED_MESSAGE"
    assert_success
}

@test "ReadUncommitted Consume ReadUncommitted message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_10" -B -d --isolation ReadUncommitted

    assert_output --partial "$READ_UNCOMMITTED_MESSAGE"
    assert_success
}

@test "Consume AtMostOnce message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_11" -B -d

    assert_output --partial "$AT_MOST_ONCE_MESSAGE"
    assert_success
}

@test "Consume AtLeastOnce message" {
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME_12" -B -d

    assert_output --partial "$AT_LEAST_ONCE_MESSAGE"
    assert_success
}
