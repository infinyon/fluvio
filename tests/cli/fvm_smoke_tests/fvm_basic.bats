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

    # Ensure the versions dir is available
    test -d ~/.fvm/versions
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

@test "Install Fluvio at 0.10.15" {
    $FVM_BIN self install

    # Output install logs
    export HUB_REGISTRY_URL="https://hub-dev.infinyon.cloud"
    run bash -c '$FVM_BIN install 0.10.15'
    assert_success

    # Ensure the stable version dir is available
    test -d ~/.fvm/versions/0.10.15
    assert_success

    # Verify fluvio binary is present
    test -f ~/.fvm/versions/0.10.15/fluvio
    assert_success

    # Verify fluvio-run binary is present
    test -f ~/.fvm/versions/0.10.15/fluvio-run
    assert_success

    # Verify fluvio-cloud binary is present
    test -f ~/.fvm/versions/0.10.15/fluvio-cloud
    assert_success

    # Verify cdk binary is present
    test -f ~/.fvm/versions/0.10.15/cdk
    assert_success

    # Verify smdk binary is present
    test -f ~/.fvm/versions/0.10.15/smdk
    assert_success

    # Check mainfest matches
    run bash -c 'cat ~/.fvm/versions/0.10.15/manifest.json | jq .channel.tag'
    assert_output "0.10.15"
    assert_success

    run bash -c 'cat ~/.fvm/versions/0.10.15/manifest.json | jq .channel.version'
    assert_output "0.10.15"
    assert_success

    # Check downloaded Fluvio Version
    run bash -c '~/.fvm/versions/0.10.15/fluvio version > flv_version_0.10.15.out && cat flv_version_0.10.15.out | head -n 1 | grep "0.10.15"'
    assert_output --partial "0.10.15"
    assert_success

    # Removes FVM
    $FVM_BIN self uninstall
    assert_success
}
