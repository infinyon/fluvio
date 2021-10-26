#!/usr/bin/env bats

setup_file() {
    load test_helper/tools_check.bash
    load test_helper/setup_k8_cluster.bash
}

setup() {
    load test_helper/random_string.bash
    export TOPIC_NAME="$RANDOM_STRING"
}

# Create topic
@test "Create a topic" {
    run "$FLUVIO_BIN" topic create "$TOPIC_NAME"
}

# Create topic - Negative test
@test "Attempt to create a topic with same name" {
    run -1 "$FLUVIO_BIN" topic create "$TOPIC_NAME"
}

# Describe topic
@test "Describe a topic" {
    run "$FLUVIO_BIN" topic describe "$TOPIC_NAME" 
}

# Delete topic
@test "Delete a topic" {
    run "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
}

# Delete topic - Negative test 
@test "Attempt to delete a topic that doesn't exist" {
    run -1 "$FLUVIO_BIN" topic delete "$TOPIC_NAME" 
}