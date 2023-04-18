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
    
    KEY1=$(random_string)
    export KEY1
    KEY2=$(random_string)
    export KEY2
    KEY3=$(random_string)
    export KEY3
    VAL1=$(random_string)
    export VAL1
    VAL2=$(random_string)
    export VAL2
    VAL3=$(random_string)
    export VAL3
    
    SEPARATOR='||'
    export SEPARATOR
    
    MULTI_LINE_FILE_CONTENTS=$KEY1$SEPARATOR$VAL1$'\n'$KEY2$SEPARATOR$VAL2$'\n'$KEY3$SEPARATOR$VAL3
    export MULTI_LINE_FILE_CONTENTS
    
    MULTI_LINE_FILE_NAME=$(random_string)
    export MULTI_LINE_FILE_NAME
    
    run bash -c 'echo "$MULTI_LINE_FILE_CONTENTS" > "$MULTI_LINE_FILE_NAME"'
}

teardown_file() {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    run rm $MULTI_LINE_FILE_NAME
}

# Create topic
@test "Create a topic for file message with separator" {    
    debug_msg "topic: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success
}

# Produce message
@test "Produce file message with separator" {    
    run bash -c 'timeout 15s "$FLUVIO_BIN" produce --file "$MULTI_LINE_FILE_NAME" --key-separator "$SEPARATOR" "$TOPIC_NAME"'
    assert_success
}

# Consume message and compare message
# Warning: Adding anything extra to the `debug_msg` skews the message comparison
@test "Consume file message with separator" {    
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d -F '{{key}}={{value}}'

    assert_output $KEY1=$VAL1$'\n'$KEY2=$VAL2$'\n'$KEY3=$VAL3
    assert_success
}
