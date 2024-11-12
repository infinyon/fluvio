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
    TEST_DIR="$(mktemp -d -t cdk-consumer-test.XXXXX)"
    export TEST_DIR

    CONNECTOR_DIR="$(pwd)/connector/sink-test-connector"
    export CONNECTOR_DIR
}

@test "Topic with 2 partitions. Consumer reads one partition" {
    # Prepare config
    TOPIC_NAME=$(random_string)
    debug_msg "Topic name: $TOPIC_NAME"
    CONNECTOR_NAME="my-$TOPIC_NAME"
    debug_msg "Connector name: $CONNECTOR_NAME"
    export LOG_PATH="$CONNECTOR_NAME.log"
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
      count: 2
  consumer:
    partition: 1
    id: $CONNECTOR_NAME
    offset:
      strategy: manual
custom:
  api_key: api_key
  client_id: client_id
EOF
    # Test
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start --config $CONFIG_PATH --log-level info
    assert_success
    assert_output --partial "Connector runs with process id"

    wait_for_line_in_file "successfully created" $LOG_PATH 30
    wait_for_line_in_file "monitoring started" $LOG_PATH 30

    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 4:4 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"

    wait_for_line_in_file "Received record: 4" $LOG_PATH 30

    run cat $LOG_PATH

    refute_output --partial 'Received record: 1'

    run $CDK_BIN deploy shutdown --name $CONNECTOR_NAME
    assert_success
}

@test "Topic with 2 partitions. Consumer reads all partitions" {
    # Prepare config
    TOPIC_NAME=$(random_string)
    debug_msg "Topic name: $TOPIC_NAME"
    CONNECTOR_NAME="my-$TOPIC_NAME"
    debug_msg "Connector name: $CONNECTOR_NAME"
    export LOG_PATH="$CONNECTOR_NAME.log"
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
      count: 2
  consumer:
    partition: all
    id: $CONNECTOR_NAME
    offset:
      strategy: manual
custom:
  api_key: api_key
  client_id: client_id
EOF
    # Test
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start --config $CONFIG_PATH --log-level info
    assert_success
    assert_output --partial "Connector runs with process id"

    wait_for_line_in_file "successfully created" $LOG_PATH 30
    wait_for_line_in_file "monitoring started" $LOG_PATH 30

    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 4:4 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"

    wait_for_line_in_file "Received record: 4" $LOG_PATH 30
    wait_for_line_in_file "Received record: 1" $LOG_PATH 2

    run $CDK_BIN deploy shutdown --name $CONNECTOR_NAME
    assert_success
}

@test "Topic with 3 partitions. Consumer reads 2 partitions" {
    # Prepare config
    TOPIC_NAME=$(random_string)
    debug_msg "Topic name: $TOPIC_NAME"
    CONNECTOR_NAME="my-$TOPIC_NAME"
    debug_msg "Connector name: $CONNECTOR_NAME"
    export LOG_PATH="$CONNECTOR_NAME.log"
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
      count: 3
  consumer:
    id: $CONNECTOR_NAME
    offset:
      strategy: manual
    partition:
      - 1
      - 2
custom:
  api_key: api_key
  client_id: client_id
EOF
    # Test
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start --config $CONFIG_PATH --log-level info
    assert_success
    assert_output --partial "Connector runs with process id"

    wait_for_line_in_file "successfully created" $LOG_PATH 30
    wait_for_line_in_file "monitoring started" $LOG_PATH 30

    echo 3:3 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    echo 2:2 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"

    wait_for_line_in_file "Received record: 2" $LOG_PATH 30
    wait_for_line_in_file "Received record: 1" $LOG_PATH 2

    run cat $LOG_PATH
    refute_output --partial 'Received record: 3'

    run $CDK_BIN deploy shutdown --name $CONNECTOR_NAME
    assert_success
}

@test "Consumer Offsets topic exist" {
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

@test "Connector with managed consumer offsets" {
    # Prepare config
    TOPIC_NAME=$(random_string)
    CONSUMER_NAME=$(random_string)
    debug_msg "Topic name: $TOPIC_NAME"
    CONNECTOR_NAME="my-$TOPIC_NAME"
    debug_msg "Connector name: $CONNECTOR_NAME"
    export LOG_PATH="$CONNECTOR_NAME.log"
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
    partition: 0
    id: $CONSUMER_NAME
    offset:
      strategy: auto
      flush-period:
        secs: 1
        nanos: 0
custom:
  api_key: api_key
  client_id: client_id
EOF
    # Test
    cd $CONNECTOR_DIR
    run $CDK_BIN deploy --target x86_64-unknown-linux-gnu start --config $CONFIG_PATH --log-level info
    assert_success
    assert_output --partial "Connector runs with process id"

    wait_for_line_in_file "successfully created" $LOG_PATH 30
    wait_for_line_in_file "monitoring started" $LOG_PATH 30

    echo 1:1 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    sleep 2
    echo 4:4 | "$FLUVIO_BIN" produce $TOPIC_NAME --key-separator ":"
    sleep 2

    wait_for_line_in_file "Received record: 1" $LOG_PATH 30
    wait_for_line_in_file "Received record: 4" $LOG_PATH 30

    OFFSET=$("$FLUVIO_BIN" consumer list -O json | jq ".[] | select(.consumer_id == \"$CONSUMER_NAME\") | .offset")
    assert [ ! -z $OFFSET ]

    run $CDK_BIN deploy shutdown --name $CONNECTOR_NAME
    assert_success
}
