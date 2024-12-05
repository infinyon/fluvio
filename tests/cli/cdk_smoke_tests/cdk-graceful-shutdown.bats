#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    PROJECT_NAME_PREFIX="$(random_string)"
    export PROJECT_NAME_PREFIX
    TEST_DIR="$(mktemp -d -t cdk-graceful-test.XXXXX)"
    export TEST_DIR

    CONNECTOR_DIR="$(pwd)/connector/sink-test-connector"
    export CONNECTOR_DIR
}

@test "Graceful shutdown on connector with managed consumer offsets" {
    # Prepare config
    TOPIC_NAME=$(random_string)
    debug_msg "Topic name: $TOPIC_NAME"
    CONNECTOR_NAME="my-$TOPIC_NAME"
    debug_msg "Connector name: $CONNECTOR_NAME"
    export LOG_PATH="$CONNECTOR_DIR/$CONNECTOR_NAME.log"
    debug_msg "Log path: $LOG_PATH"

    CONFIG_PATH="$TEST_DIR/$TOPIC_NAME.yaml"
    cat <<EOF >$CONFIG_PATH
apiVersion: 0.2.0
meta:
  version: 0.1.0
  name: $CONNECTOR_NAME
  type: test-sink
  topic:
    meta:
      name: $TOPIC_NAME
    partition:
      count: 1
  consumer:
    partition: all
    id: $CONNECTOR_NAME
    offset:
      strategy: auto
      start: beginning
      flush-period:
        secs: 10
        nanos: 0
custom:
  api_key: api_key
  client_id: client_id
  prevent_dropped_stream: true
EOF
    # Test
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start --config $CONFIG_PATH --log-level info 3>&-
    assert_success
    assert_output --partial "Connector runs with process id"

    wait_for_line_in_file "successfully created" $LOG_PATH 5
    wait_for_line_in_file "monitoring started" $LOG_PATH 5

    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    sleep 2
    echo 2:2 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    sleep 2

    wait_for_line_in_file "Received record: 1" $LOG_PATH 5
    wait_for_line_in_file "Received record: 2" $LOG_PATH 5

    run $CDK_BIN deploy shutdown --name $CONNECTOR_NAME 3>&-
    assert_success

    echo 3:3 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 4:4 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"

    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start --config $CONFIG_PATH --log-level info 3>&-
    assert_success
    assert_output --partial "Connector runs with process id"
    wait_for_line_in_file "Received record: 3" $LOG_PATH 5
    assert_success
    wait_for_line_in_file "Received record: 4" $LOG_PATH 5

    run cat $LOG_PATH
    refute_output --partial 'Received record: 1'
    refute_output --partial 'Received record: 2'
}

