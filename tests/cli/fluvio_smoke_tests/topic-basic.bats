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
    TEST_DIR="$(mktemp -d -t topic-basic-test.XXXXX)"

    TOPIC_CONFIG_PATH="$TEST_DIR/$TOPIC_NAME.yaml"
    export TOPIC_CONFIG_PATH

    REPLICA_CONFIG_PATH="$TEST_HELPER_DIR/replica.json"
    export REPLICA_CONFIG_PATH

    TOPIC_NAME_REPLICA=$(random_string)
    export TOPIC_NAME_REPLICA

    DEDUP_FILTER_NAME="dedup-filter"
    export DEDUP_FILTER_NAME

    cat <<EOF >$TOPIC_CONFIG_PATH
version: 0.1.0
meta:
  name: $TOPIC_NAME
partition:
  count: 1
  max_size: 10 KB
  replication: 1
  ignore_rack_assignment: true
retention:
  time: 2m
  segment_size: 2.0 KB
compression:
  type: Lz4
deduplication:
  bounds:
    count: 100
    age: 1m
  filter:
    transform:
      uses: $DEDUP_FILTER_NAME  
EOF

    run timeout 15s "$FLUVIO_BIN" sm create --wasm-file smartmodule/examples/target/wasm32-unknown-unknown/release/fluvio_smartmodule_filter.wasm "$DEDUP_FILTER_NAME"
    assert_success
}

# Create topic
@test "Create a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" 
    #debug_msg "command $BATS_RUN_COMMAND" # This doesn't do anything.
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create topic with replic assigmment
@test "Create a topic with replica assignment" {
    # skip stable since it format changes
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    debug_msg "Topic name: $TOPIC_NAME_REPLICA"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME_REPLICA" --replica-assignment "$REPLICA_CONFIG_PATH"
    #debug_msg "command $BATS_RUN_COMMAND" # This doesn't do anything.
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create topic - Negative test
@test "Attempt to create a topic with same name" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME"
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "Topic already exists"
}

# Describe topic
@test "Describe a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic describe "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete topic
@test "Delete a topic" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Delete topic - Negative test 
@test "Attempt to delete a topic that doesn't exist" {
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "Topic not found"
}

# Create topic with max partition size (dry run)
@test "Attempt to create topic with specified max partition size" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --max-partition-size 10Gb --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create topic with segment size (dry run)
@test "Attempt to create topic with specified segment size" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --segment-size "2 Ki" --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Create topic with too small max partition size (dry run) - Negative test
@test "Attempt to create topic with too small max partition size" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --max-partition-size "10" --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "max_partition_size 10 is less than minimum 2048"
}

# Create topic with max partition size is less than segment size (dry run) - Negative test
@test "Attempt to create topic with max partition size smaller than segment size" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --segment-size "3 Ki" --max-partition-size "2 Ki" --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "max_partition_size 2048 is less than segment size 3072"
}

# Create topic with empty name - Negative test
@test "Attempt to create topic with empty name" {
    run timeout 15s "$FLUVIO_BIN" topic create " " --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "Topic name is required"
}


# Create topic with name and config file - Negative test
@test "Attempt to create topic with name and config file" {
    run timeout 15s "$FLUVIO_BIN" topic create "$(random_string)" --config /tmp/config.yaml --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "error: the argument '--config <PATH>' cannot be used with"
}

# Create topic with partition size and config file - Negative test
@test "Attempt to create topic with partition size and config file" {
    run timeout 15s "$FLUVIO_BIN" topic create --max-partition-size "2 Ki"  --config /tmp/config.yaml --dry-run
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_failure
    assert_output --partial "error: the argument '--config <PATH>' cannot be used with"
}

# Create topic from config file
@test "Create a topic from config file" {
    debug_msg "Topic config file: $TOPIC_CONFIG_PATH"
    run timeout 15s "$FLUVIO_BIN" topic create --config "$TOPIC_CONFIG_PATH" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
}

# Describe topic contains Deduplication info
@test "Describe a topic with deduplication info" {
    if [ "$FLUVIO_CLI_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on fluvio cli stable version"
    fi
    if [ "$FLUVIO_CLUSTER_RELEASE_CHANNEL" == "stable" ]; then
        skip "don't run on cluster stable version"
    fi
    debug_msg "Topic name: $TOPIC_NAME"
    run timeout 15s "$FLUVIO_BIN" topic describe "$TOPIC_NAME" 
    debug_msg "status: $status"
    debug_msg "output: ${lines[0]}"
    assert_success
    assert_output --partial "Deduplication Filter:$DEDUP_FILTER_NAME"
    assert_output --partial "Deduplication Count Bound:10"
    assert_output --partial "Deduplication Age Bound:1"
}
