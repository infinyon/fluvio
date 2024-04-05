#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

run_list_spus() {
    # Pipe the CLI into jq to control the output.
    # We must fail with `fluvio cluster` (hence -o pipefail) to propagate whether the command failed.
    run bash -o pipefail -c "\"$FLUVIO_BIN\" cluster spu list -O json | jq 'length'"
}

run_resume() {
    # Since `fluvio run sc` runs sc as a child process without daemonizing it, all the FDs
    # are inherited to the children. That includes FD3 and other FDs that causes the test to hang. 
    # see: https://bats-core.readthedocs.io/en/stable/writing-tests.html#file-descriptor-3-read-this-if-bats-hangs
    # To circumvent it, we kill the parent process (with nohup) and close all of the inherited file descriptors.
    # Note the current approach is hacky because it blindly closes FDs in the range of [3, 100]
    # An alternative approach would be to list and close only the non 0,1,2 FDs. However it bloats the test.
    # Example: https://github.com/bats-core/bats-core/blob/master/test/fixtures/bats/issue-205.bats
    # TODO: Consider daemonizing the local cluster SC, which should solve this problem properly.
    close_fd_cmd='for fd in $(seq 3 100); do eval exec "$fd>&-"; done'
    run_resume_cmd="nohup $FLUVIO_BIN cluster resume 0<&-"
    run bash -c "${close_fd_cmd} && ${run_resume_cmd} &" 3>&-
}

setup_file() {
    run_list_spus
    # Expecting to have a running cluster
    assert_success
    CLUSTER_SPUS=$output
    export CLUSTER_SPUS

    BATS_TEST_TIMEOUT=10
    export BATS_TEST_TIMEOUT
}

@test "Resume SPU instances" {
    run timeout 15s "$FLUVIO_BIN" cluster shutdown
    assert_success

    run_list_spus
    # Cluster is not running so run should fail
    assert_failure

    run_resume

    for retry in $(seq 1 5); do
        run_list_spus
        if [ "$status" -ne 0 ]; then 
            debug_msg "retry listing SPUs..."
            sleep 1
        else
            debug_msg "Got $output SPUs after ${retry} retries. It is expected to be the same as $CLUSTER_SPUS"
            current_spus=$output
            break;
        fi;
    done;

    assert_equal $CLUSTER_SPUS $current_spus
}
