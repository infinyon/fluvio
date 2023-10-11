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
    # Tests in this file are executed in order and rely on the previous test
    # to be successful.

    # Retrieves the latest stable version from the GitHub API and removes the
    # `v` prefix from the tag name.
    STABLE_VERSION=$(curl "https://api.github.com/repos/infinyon/fluvio/releases/latest" | jq -r .tag_name | cut -c2-)
    export STABLE_VERSION
    debug_msg "Stable Version: $STABLE_VERSION"

    # The directory where FVM stores the downloaded versions
    VERSIONS_DIR="$HOME/.fvm/versions"
    export VERSIONS_DIR
    debug_msg "Versions Directory: $VERSIONS_DIR"
}


@test "Install fvm and setup a settings.toml file" {
    # Ensure the `fvm` directory is not present
    run bash -c '! test -d ~/.fvm'
    assert_success

    # Installs FVM which introduces the `~/.fvm` directory and copies the FVM
    # binary to ~/.fvm/bin/fvm
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Tests FVM to be in the PATH
    run bash -c 'which fvm'
    assert_output --partial ".fvm/bin/fvm"
    assert_success

    # Retrieves Version from FVM
    run bash -c 'fvm --help'
    assert_output --partial "Fluvio Version Manager (FVM)"
    assert_success

    # Ensure the `settings.toml` is available. At this point this is an empty file
    run bash -c 'cat ~/.fvm/settings.toml'
    assert_output ""
    assert_success
}

@test "Uninstall fvm and removes ~/.fvm dir" {
    # Ensure the `fvm` directory is present from the previous test
    run bash -c 'test -d ~/.fvm'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Test the fvm command is present
    run bash -c 'which fvm'
    assert_output --partial ".fvm/bin/fvm"
    assert_success

    # We use `--yes` because prompting is not supported in CI environment,
    # responding with error `Error: IO error: not a terminal`
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Ensure the `~/.fvm/` directory is not available anymore
    run bash -c '! test -d ~/.fvm'
    assert_success

    # Ensure the fvm is not available anymore
    run bash -c '! fvm'
    assert_success
}

@test "Install Fluvio at 0.10.15" {
    # Ensure the `~/.fvm/` directory is not available anymore
    run bash -c '! test -d ~/.fvm'
    assert_success

    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    run bash -c 'fvm install 0.10.15'
    assert_success

    # Verify fluvio binary is present
    run bash -c 'test -f $VERSIONS_DIR/0.10.15/fluvio'
    assert_success

    # Verify fluvio-run binary is present
    run bash -c 'test -f $VERSIONS_DIR/0.10.15/fluvio-run'
    assert_success

    # Verify fluvio-cloud binary is present
    run bash -c 'test -f $VERSIONS_DIR/0.10.15/fluvio-cloud'
    assert_success

    # Verify cdk binary is present
    run bash -c 'test -f $VERSIONS_DIR/0.10.15/cdk'
    assert_success

    # Verify smdk binary is present
    run bash -c 'test -f $VERSIONS_DIR/0.10.15/smdk'
    assert_success

    # Check mainfest matches
    run bash -c 'cat $VERSIONS_DIR/0.10.15/manifest.json | jq .channel.tag'
    assert_output "\"0.10.15\""
    assert_success

    run bash -c 'cat $VERSIONS_DIR/0.10.15/manifest.json | jq .version'
    assert_output "\"0.10.15\""
    assert_success

    # Check downloaded Fluvio Version
    run bash -c '$VERSIONS_DIR/0.10.15/fluvio version > flv_version_0.10.15.out && cat flv_version_0.10.15.out | head -n 1 | grep "0.10.15"'
    assert_output --partial "0.10.15"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success
}

@test "Install Stable Fluvio" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Installs Stable Fluvio
    run bash -c 'fvm install'
    assert_success

    # Ensure the stable version dir is available
    run bash -c 'test -d $VERSIONS_DIR/stable'
    assert_success

    # Verify fluvio binary is present
    run bash -c 'test -f $VERSIONS_DIR/stable/fluvio'
    assert_success

    # Verify fluvio-run binary is present
    run bash -c 'test -f $VERSIONS_DIR/stable/fluvio-run'
    assert_success

    # Verify fluvio-cloud binary is present
    run bash -c 'test -f $VERSIONS_DIR/stable/fluvio-cloud'
    assert_success

    # Verify cdk binary is present
    run bash -c 'test -f $VERSIONS_DIR/stable/cdk'
    assert_success

    # Verify smdk binary is present
    run bash -c 'test -f $VERSIONS_DIR/stable/smdk'
    assert_success

    # Check mainfest matches
    run bash -c 'cat $VERSIONS_DIR/stable/manifest.json | jq .channel'
    assert_output "\"stable\""
    assert_success

    run bash -c 'cat $VERSIONS_DIR/stable/manifest.json | jq .version'
    assert_output "\"$STABLE_VERSION\""
    assert_success

    # Check downloaded Fluvio Version
    run bash -c '$VERSIONS_DIR/stable/fluvio version > flv_version_stable.out && cat flv_version_stable.out | head -n 1 | grep "$STABLE_VERSION"'
    assert_output --partial "$STABLE_VERSION"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success
}

@test "Creates the `$VERSIONS_DIR` path if not present when attempting to install" {
    # Verify the directory is not present initally
    run bash -c '! test -d $VERSIONS_DIR'
    assert_success

    # Installs FVM as usual
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Verify the directory is now present
    run bash -c 'test -d $VERSIONS_DIR'
    assert_success

    # Remove versions directory
    rm -rf $VERSIONS_DIR

    # Installs Stable Fluvio
    run bash -c 'fvm install'
    assert_success

    # Checks the presence of the binary in the versions directory
    run bash -c 'test -f $VERSIONS_DIR/stable/fluvio'
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success
}
