#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    # Compile the smartmodule examples
    pushd "$BATS_TEST_DIRNAME/../../.." && make build_smartmodules && popd
    SMARTMODULE_BUILD_DIR="$BATS_TEST_DIRNAME/../../../crates/fluvio-smartmodule/examples/target/wasm32-unknown-unknown/release/"
    export SMARTMODULE_BUILD_DIR
    
}

@test "smartmodule map" {
    # Load the smartmodule
    SMARTMODULE_NAME="uppercase"
    export SMARTMODULE_NAME
    run timeout 15s "$FLUVIO_BIN" smartmodule create $SMARTMODULE_NAME --wasm-file $SMARTMODULE_BUILD_DIR/fluvio_wasm_map.wasm 
    assert_success

    # Create topic
    TOPIC_NAME="$(random_string)"
    export TOPIC_NAME
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success

    # Produce to topic
    TEST_MESSAGE="$(random_string 10)"
    export TEST_MESSAGE
    run bash -c 'echo "$TEST_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success

    # Consume from topic
    EXPECTED_OUTPUT="${TEST_MESSAGE^^}"
    export EXPECTED_OUTPUT
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d --map "$SMARTMODULE_NAME"

    assert_output --partial "$EXPECTED_OUTPUT"
    assert_success

    # Delete topic
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_success

    # Delete smartmodule
    run timeout 15s "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    assert_success
}

@test "smartmodule filter" {
    # Load the smartmodule
    SMARTMODULE_NAME="contains-a"
    export SMARTMODULE_NAME
    run timeout 15s "$FLUVIO_BIN" smartmodule create $SMARTMODULE_NAME --wasm-file $SMARTMODULE_BUILD_DIR/fluvio_wasm_filter.wasm 
    assert_success

    # Create topic
    TOPIC_NAME="$(random_string)"
    export TOPIC_NAME
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success

    # Produce to topic
    NEGATIVE_TEST_MESSAGE="zzzzzzzzzzzzzz"
    export NEGATIVE_TEST_MESSAGE
    run bash -c 'echo "$NEGATIVE_TEST_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success

    TEST_MESSAGE="$(random_string 10)aaa"
    export TEST_MESSAGE
    run bash -c 'echo "$TEST_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success

    # Consume from topic and verify we should have 2 entries
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d
    assert_line --index 0 "$NEGATIVE_TEST_MESSAGE"
    assert_line --index 1 "$TEST_MESSAGE"

    # Consume from topic with smartmodule and verify we don't see the $NEGATIVE_TEST_MESSAGE
    EXPECTED_OUTPUT="${TEST_MESSAGE}"
    export EXPECTED_OUTPUT
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d --filter "$SMARTMODULE_NAME"
    refute_line "$NEGATIVE_TEST_MESSAGE"
    assert_output "$EXPECTED_OUTPUT"

    # Delete topic
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_success

    # Delete smartmodule
    run timeout 15s "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    assert_success
}

@test "smartmodule filter w/ params" {
    # Load the smartmodule
    SMARTMODULE_NAME="contains-a-or-param"
    export SMARTMODULE_NAME
    run timeout 15s "$FLUVIO_BIN" smartmodule create $SMARTMODULE_NAME --wasm-file $SMARTMODULE_BUILD_DIR/fluvio_wasm_filter_with_parameters.wasm 
    assert_success

    # Create topic
    TOPIC_NAME="$(random_string)"
    export TOPIC_NAME
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success

    # Produce to topic
    TEST_MESSAGE="$(random_string 10)"
    export TEST_MESSAGE
    run bash -c 'echo "$TEST_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success

    # Consume from topic
    #EXPECTED_OUTPUT="${TEST_MESSAGE^^}"
    #export EXPECTED_OUTPUT
    #run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d --filter "$SMARTMODULE_NAME" -e key=b

    #assert_output --partial "$EXPECTED_OUTPUT"
    #assert_success

    # Delete topic
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_success

    # Delete smartmodule
    run timeout 15s "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    assert_success
}

@test "smartmodule filter-map" {
    # Load the smartmodule
    SMARTMODULE_NAME="divide-even-by-2"
    export SMARTMODULE_NAME
    run timeout 15s "$FLUVIO_BIN" smartmodule create $SMARTMODULE_NAME --wasm-file $SMARTMODULE_BUILD_DIR/fluvio_wasm_filter_map.wasm 
    assert_success

    # Create topic
    TOPIC_NAME="$(random_string)"
    export TOPIC_NAME
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success

    # Produce to topic
    TEST_MESSAGE="$(random_string 10)"
    export TEST_MESSAGE
    run bash -c 'echo "$TEST_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success

    # Consume from topic
    #EXPECTED_OUTPUT="${TEST_MESSAGE^^}"
    #export EXPECTED_OUTPUT
    #run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d --filter-map "$SMARTMODULE_NAME"

    #assert_output --partial "$EXPECTED_OUTPUT"
    #assert_success

    # Delete topic
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_success

    # Delete smartmodule
    run timeout 15s "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    assert_success
}

@test "smartmodule array-map" {
    # Load the smartmodule
    SMARTMODULE_NAME="divide-even-by-2"
    export SMARTMODULE_NAME
    run timeout 15s "$FLUVIO_BIN" smartmodule create $SMARTMODULE_NAME --wasm-file $SMARTMODULE_BUILD_DIR/fluvio_wasm_filter_map.wasm 
    assert_success

    # Create topic
    TOPIC_NAME="$(random_string)"
    export TOPIC_NAME
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success

    # Produce to topic
    TEST_MESSAGE="$(random_string 10)"
    export TEST_MESSAGE
    run bash -c 'echo "$TEST_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success

    # Consume from topic
    #EXPECTED_OUTPUT="${TEST_MESSAGE^^}"
    #export EXPECTED_OUTPUT
    #run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d --array-map "$SMARTMODULE_NAME"

    #assert_output --partial "$EXPECTED_OUTPUT"
    #assert_success

    # Delete topic
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_success

    # Delete smartmodule
    run timeout 15s "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    assert_success
}

@test "smartmodule aggregate" {
    # Load the smartmodule
    SMARTMODULE_NAME="concat-strings"
    export SMARTMODULE_NAME
    run timeout 15s "$FLUVIO_BIN" smartmodule create $SMARTMODULE_NAME --wasm-file $SMARTMODULE_BUILD_DIR/fluvio_wasm_aggregate.wasm 
    assert_success

    # Create topic
    TOPIC_NAME="$(random_string)"
    export TOPIC_NAME
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    assert_success

    # Produce to topic
    TEST_MESSAGE="$(random_string 10)"
    export TEST_MESSAGE
    run bash -c 'echo "$TEST_MESSAGE" | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success

    # Consume from topic
    #EXPECTED_OUTPUT="${TEST_MESSAGE^^}"
    #export EXPECTED_OUTPUT
    #run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -B -d --aggregate "$SMARTMODULE_NAME"

    #assert_output --partial "$EXPECTED_OUTPUT"
    #assert_success

    # Delete topic
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"
    assert_success

    # Delete smartmodule
    run timeout 15s "$FLUVIO_BIN" smartmodule delete "$SMARTMODULE_NAME"
    assert_success
}


# Expect that we already have a 2 wasm modules. One each for positive and negative test

# Create topic
# Produce known records 

# Positive test

# Consume topic w/ smartstream and compare output is less than total records, and meets 

# Negative test

# Consume topic w/ smartstream and confirm no output 