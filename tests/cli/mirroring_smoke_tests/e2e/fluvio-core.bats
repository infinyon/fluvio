#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    CURRENT_DATE=$(date +%Y-%m)
    export CURRENT_DATE

    REMOTE_PROFILE=local
    export REMOTE_PROFILE
    debug_msg "Remote profile: $REMOTE_PROFILE"

    REMOTE_NAME=edge1
    export REMOTE_NAME
    debug_msg "Remote name: $REMOTE_NAME"

    REMOTE_PROFILE_2=local2
    export REMOTE_PROFILE_2
    debug_msg "Remote profile: $REMOTE_PROFILE_2"

    REMOTE_NAME_2=edge2
    export REMOTE_NAME_2
    debug_msg "Remote name: $REMOTE_NAME_2"

    MESSAGE="$(random_string 7)"
    export MESSAGE
    debug_msg "$MESSAGE"

    TOPIC_NAME=mirror-topic
    export TOPIC_NAME
    debug_msg "Topic name: $TOPIC_NAME"

    HOME_PROFILE=$($FLUVIO_BIN profile)
    export HOME_PROFILE
    debug_msg "Home profile: $HOME_PROFILE"

    HOME_NAME="home"
    export HOME_NAME
    debug_msg "Home name: $HOME_NAME"\

    K8S_DEFAULT_PORT=":30003"
    export K8S_DEFAULT_PORT
    debug_msg "K8S default port: $K8S_DEFAULT_PORT"
}

@test "Can register first local remote cluster" {
    run timeout 15s "$FLUVIO_BIN" remote register "$REMOTE_NAME"

    assert_output "remote cluster \"$REMOTE_NAME\" was registered"
    assert_success
}

@test "Can register second local remote cluster" {
    run timeout 15s "$FLUVIO_BIN" remote register "$REMOTE_NAME_2"

    assert_output "remote cluster \"$REMOTE_NAME_2\" was registered"
    assert_success
}

@test "Export home to remote 1" {
    run timeout 15s "$FLUVIO_BIN" remote export "$REMOTE_NAME" --file remote.json
    assert_output ""
    assert_success

    run jq -r .home.id remote.json 
    assert_output "$HOME_NAME"
    assert_success

    run jq -r .home.remoteId remote.json 
    assert_output "$REMOTE_NAME"
    assert_success

    run jq -r .home.publicEndpoint remote.json 
    assert_output --partial "$K8S_DEFAULT_PORT"
    assert_success
}

@test "Export home to remote 2" {
    run timeout 15s "$FLUVIO_BIN" remote export "$REMOTE_NAME_2" --file remote2.json
    assert_output ""
    assert_success

    run jq -r .home.id remote2.json 
    assert_output "$HOME_NAME"
    assert_success

    run jq -r .home.remoteId remote2.json 
    assert_output "$REMOTE_NAME_2"
    assert_success

    run jq -r .home.publicEndpoint remote2.json 
    assert_output --partial "$K8S_DEFAULT_PORT"
    assert_success
}

@test "Can create a mirror topic with the remote clusters" {
    echo "[\"$REMOTE_NAME\",\"$REMOTE_NAME_2\"]" > remotes_devices.json
    run timeout 15s "$FLUVIO_BIN" topic create "$TOPIC_NAME" --mirror-apply remotes_devices.json

    assert_output "topic \"$TOPIC_NAME\" created"
    assert_success
}

@test "Can switch to remote cluster 1" {
    run timeout 15s "$FLUVIO_BIN" profile switch "$REMOTE_PROFILE"
    assert_output ""
    assert_success
}

#TODO: actually we should have the topics created by the export file too.
@test "Can't produce message to mirror topic from remote 1 before connect to home" {
    sleep 5
    run bash -c 'echo 1 | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_output "Topic not found: $TOPIC_NAME"
    assert_failure
}

@test "Can connect to the home cluster from remote 1" {
    sleep 2
    run timeout 15s "$FLUVIO_BIN" home connect --file remote.json

    assert_output "connecting with \"$HOME_NAME\" cluster"
    assert_success
}

@test "Home status at remote 1 should show the home cluster connected" {
    sleep 60
    run timeout 15s "$FLUVIO_BIN" home status

    assert_success
    assert_line --partial --index 1 "Connected  Connected"
    assert [ ${#lines[@]} -eq 2 ]
}

@test "Can produce message to mirror topic from remote 1" {
    sleep 2
    run bash -c 'echo 1 | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
    run bash -c 'echo a | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
    run bash -c 'echo 2 | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
    run bash -c 'echo b | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
}

@test "Can switch to remote cluster 2" {
    run timeout 15s "$FLUVIO_BIN" profile switch "$REMOTE_PROFILE_2"
    assert_output ""
    assert_success
}

@test "Can connect to the home cluster from remote 2" {
    sleep 2
    run timeout 15s "$FLUVIO_BIN" home connect --file remote2.json

    assert_output "connecting with \"$HOME_NAME\" cluster"
    assert_success
}

@test "Home status at remote 2 should show the home cluster connected" {
    sleep 60
    run timeout 15s "$FLUVIO_BIN" home status

    assert_success
    assert_line --partial --index 1 "Connected  Connected"
    assert [ ${#lines[@]} -eq 2 ]
}

@test "Can produce message to mirror topic" {
    sleep 2
    run bash -c 'echo 9 | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
    run bash -c 'echo z | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
    run bash -c 'echo 8 | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
    run bash -c 'echo y | timeout 15s "$FLUVIO_BIN" produce "$TOPIC_NAME"'
    assert_success
}

@test "Can't delete mirror topic from remote 2" {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"

    assert_output "cannot delete mirrored topic from remote"
    assert_failure
}

@test "Can switch back to home cluster" {
    run timeout 15s "$FLUVIO_BIN" profile switch "$HOME_PROFILE"
    assert_output ""
    assert_success
}

@test "Remote list should show the remote clusters connected" {
    sleep 2
    run timeout 15s "$FLUVIO_BIN" remote list

    assert_success
    assert_line --partial --index 1 "Connected  Connected"
    assert_line --partial --index 2 "Connected  Connected"
    assert [ ${#lines[@]} -eq 3 ]
}

@test "Can consume message from mirror topic produced from remote 1 by partition or remote" {
    sleep 5
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -p 0 -B -d
    assert_output 1$'\n'a$'\n'2$'\n'b
    assert_success

    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --mirror "$REMOTE_NAME" -B -d
    assert_output 1$'\n'a$'\n'2$'\n'b
    assert_success
}

@test "Can consume message from mirror topic produced from remote 2 by partition or remote" {
    sleep 5
    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" -p 1 -B -d
    assert_output 9$'\n'z$'\n'8$'\n'y
    assert_success

    run timeout 15s "$FLUVIO_BIN" consume "$TOPIC_NAME" --mirror "$REMOTE_NAME_2" -B -d
    assert_output 9$'\n'z$'\n'8$'\n'y
    assert_success
}

#TODO: after implement disconnect command, create a test to disconnect home from remote 2, then produce 2nd message on remote 2

@test "Can delete mirror topic" {
    run timeout 15s "$FLUVIO_BIN" topic delete "$TOPIC_NAME"

    assert_output "topic \"$TOPIC_NAME\" deleted"
    assert_success
}

@test "Can switch to remote cluster 1 and check if the mirror topic is deleted" {
    run timeout 15s "$FLUVIO_BIN" profile switch "$REMOTE_PROFILE"
    assert_output ""
    assert_success

    sleep 6
    run timeout 15s "$FLUVIO_BIN" topic list
    assert_output --partial "No topics found"
    assert_success
}

@test "Can switch to remote cluster 2 and check if the mirror topic is deleted" {
    run timeout 15s "$FLUVIO_BIN" profile switch "$REMOTE_PROFILE_2"
    assert_output ""
    assert_success

    sleep 6
    run timeout 15s "$FLUVIO_BIN" topic list
    assert_output --partial "No topics found"
    assert_success
}

