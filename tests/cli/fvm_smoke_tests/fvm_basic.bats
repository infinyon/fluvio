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
    assert_success

    # Ensure the `~/.fvm/` directory is available
    test -d ~/.fvm
    assert_success

    # Ensure the `fvm` binary is available
    test -f ~/.fvm/bin/fvm
    assert_success

    # Ensure the `settings.toml` is available
    test -f ~/.fvm/settings.toml
    assert_success

    # Ensure the `env` file is available
    test -f ~/.fvm/env
    assert_success

    # Ensure the package set dir is available
    test -d ~/.fvm/pkgset
    assert_success
}

@test "Uninstall fvm and removes ~/.fvm dir" {
    # We use `--yes` because prompting is not supported in CI environment,
    # responding with error `Error: IO error: not a terminal`

    run $FVM_BIN self uninstall --yes
    assert_success

    # Ensure the `~/.fvm/` directory is not available anymore
    ! test -d ~/.fvm
    assert_success
}
