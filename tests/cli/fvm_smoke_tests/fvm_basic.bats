#!/usr/bin/env bats

SKIP_CLUSTER_START=true
export SKIP_CLUSTER_START

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
  echo "No-Op"
}

@test "Install fvm and setup a settings.toml file" {
    run $FVM_BIN self install
    assert_line --index 0 "done: FVM installed successfully at /home/runner/.fvm"
    assert_line --index 1 "help: Add FVM to PATH using source $HOME/.fvm/env"
    assert_success

    # The file is going to be present but is going to be empty at first given
    # that at this point no fluvio version is installed by FVM
    cat  ~/.fvm/settings.toml
    assert_success

    # Ensure the `fvm` binary is available
    ~/.fvm/bin/fvm
    assert_success
}

@test "Uninstall fvm and removes ~/.fvm dir" {
    # We use `--yes` because prompting is not supported in CI environment,
    # responding with error `Error: IO error: not a terminal`

    run $FVM_BIN self uninstall --yes
    assert_output --partial "Fluvio Version Manager was removed from"
    assert_success

    run cd ~/.fvm
    assert_output --partial "cd: /home/runner/.fvm: No such file or directory"
}
