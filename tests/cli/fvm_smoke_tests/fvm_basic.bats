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

    # The directory where FVM files live
    FVM_HOME_DIR="$HOME/.fvm"
    export FVM_HOME_DIR
    debug_msg "FVM Home Directory: $FVM_HOME_DIR"

    # The directory where FVM stores the downloaded versions
    VERSIONS_DIR="$FVM_HOME_DIR/versions"
    export VERSIONS_DIR
    debug_msg "Versions Directory: $VERSIONS_DIR"

    # The path to the Settings Toml file
    SETTINGS_TOML_PATH="$FVM_HOME_DIR/settings.toml"
    export SETTINGS_TOML_PATH
    debug_msg "Settings Toml Path: $SETTINGS_TOML_PATH"

    STATIC_VERSION="0.10.15"
    export STATIC_VERSION
    debug_msg "Static Version: $STATIC_VERSION"

    FLUVIO_HOME_DIR="$HOME/.fluvio"
    export FLUVIO_HOME_DIR
    debug_msg "Fluvio Home Directory: $FLUVIO_HOME_DIR"

    FLUVIO_BINARIES_DIR="$FLUVIO_HOME_DIR/bin"
    export FLUVIO_BINARIES_DIR
    debug_msg "Fluvio Binaries Directory: $FLUVIO_BINARIES_DIR"
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

@test "Creates the `$VERSIONS_DIR` path if not present when attempting to install" {
    # Verify the directory is not present initally
    run bash -c '! test -d $VERSIONS_DIR'
    assert_success

    # Installs FVM as usual
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Renders warn if attempts to use `fvm show` and no versions are installed
    run bash -c 'fvm show'
    assert_line --index 0 "warn: No installed versions found"
    assert_line --index 1 "help: You can install a Fluvio version using the command fvm install"
    assert_success

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

@test "Install Fluvio Versions" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Expected binaries
    declare -a binaries=(
        fluvio
        fluvio-run
        fluvio-cloud
        cdk
        smdk
    )

    # Expected versions
    declare -a versions=(
        $STATIC_VERSION
        stable
        latest
    )

    for version in "${versions[@]}"
    do
        export VERSION="$version"

        run bash -c 'fvm install "$VERSION"'
        assert_success

        for binary in "${binaries[@]}"
        do
            export BINARY_PATH="$VERSIONS_DIR/$VERSION/$binary"
            echo "Checking binary: $BINARY_PATH"
            run bash -c 'test -f $BINARY_PATH'
            assert_success
        done

        if [ "$VERSION" == "stable" ] || [ "$VERSION" == "latest" ]; then
            run bash -c 'cat "$VERSIONS_DIR/$VERSION/manifest.json" | jq .channel'
            assert_output "\"$version\""
            assert_success
        else
            run bash -c 'cat "$VERSIONS_DIR/$VERSION/manifest.json" | jq .version'
            assert_output "\"$version\""
            assert_success
        fi

        if [ "$VERSION" == "stable" ]; then
            run bash -c '$VERSIONS_DIR/$VERSION/fluvio version > flv_version_$version.out && cat flv_version_$version.out | head -n 1 | grep "$STABLE_VERSION"'
            assert_output --partial "$STABLE_VERSION"
            assert_success
        fi

        if [ "$VERSION" == "$STATIC_VERSION" ]; then
            run bash -c '$VERSIONS_DIR/$VERSION/fluvio version > flv_version_$version.out && cat flv_version_$version.out | head -n 1 | grep "$STATIC_VERSION"'
            assert_output --partial "$STATIC_VERSION"
            assert_success
        fi
    done

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success
}

@test "Copies binaries to Fluvio Binaries Directory when using fvm switch" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Removes Fluvio Directory
    rm -rf $FLUVIO_HOME_DIR

    # Ensure `~/.fluvio` is not present
    run bash -c '! test -d $FLUVIO_HOME_DIR'
    assert_success

    declare -a versions=(
        stable
        $STATIC_VERSION
    )

    # Installs Versions
    for version in "${versions[@]}"
    do
        export VERSION="$version"

        # Installs Fluvio Version
        run bash -c 'fvm install $VERSION'
        assert_success
    done

    for version in "${versions[@]}"
    do
        export VERSION="$version"

        # Switch version to use
        run bash -c 'fvm switch $VERSION'
        assert_success

        # Checks version is set
        if [ "$VERSION" == "stable" ]; then
            run bash -c 'fluvio version > flv_version_$version.out && cat flv_version_$version.out | head -n 1 | grep "$STABLE_VERSION"'
            assert_output --partial "$STABLE_VERSION"
            assert_success
        fi

        if [ "$VERSION" == "$STATIC_VERSION" ]; then
            run bash -c 'fluvio version > flv_version_$version.out && cat flv_version_$version.out | head -n 1 | grep "$STATIC_VERSION"'
            assert_output --partial "$STATIC_VERSION"
            assert_success
        fi

        # Checks Settings File's Version is updated
        if [ "$VERSION" == "stable" ]; then
            run bash -c 'yq -oy '.version' "$SETTINGS_TOML_PATH"'
            assert_output --partial "$STABLE_VERSION"
            assert_success
        fi

        if [ "$VERSION" == "$STATIC_VERSION" ]; then
            run bash -c 'yq -oy '.version' "$SETTINGS_TOML_PATH"'
            assert_output --partial "$STATIC_VERSION"
            assert_success
        fi

        # Checks Settings File's Channel is updated
        if [ "$VERSION" == "stable" ]; then
            run bash -c 'yq -oy '.channel' "$SETTINGS_TOML_PATH"'
            assert_output --partial "stable"
            assert_success
        fi

        if [ "$VERSION" == "$STATIC_VERSION" ]; then
            run bash -c 'yq -oy '.channel.tag' "$SETTINGS_TOML_PATH"'
            assert_output --partial "$STATIC_VERSION"
            assert_success
        fi

        # Expected binaries
        declare -a binaries=(
            fluvio
            fluvio-run
            fluvio-cloud
            cdk
            smdk
        )

        for binary in "${binaries[@]}"
        do
            export FVM_BIN_PATH="$VERSIONS_DIR/$VERSION/$binary"
            echo "Checking binary: $BINARY_PATH"
            run bash -c 'test -f $FVM_BIN_PATH'
            assert_success

            export FLV_BIN_PATH="$FLUVIO_BINARIES_DIR/$binary"
            echo "Checking binary: $FLV_BIN_PATH"
            run bash -c 'test -f $FLV_BIN_PATH'
            assert_success

            export SHASUM_A=$(sha256sum $FVM_BIN_PATH | awk '{ print $1 }')
            export SHASUM_B=$(sha256sum $FLV_BIN_PATH | awk '{ print $1 }')
            assert_equal "$SHASUM_A" "$SHASUM_B"
        done
    done

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Sets the desired Fluvio Version" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Ensure `~/.fluvio` is not present
    run bash -c '! test -d $FLUVIO_HOME_DIR'
    assert_success

    # Installs Fluvio at 0.10.15
    run bash -c 'fvm install 0.10.15'
    assert_success

    # Installs Fluvio at 0.10.14
    run bash -c 'fvm install 0.10.14'
    assert_success

    # Switch version to use Fluvio at 0.10.15
    run bash -c 'fvm switch 0.10.15'
    assert_success

    # Checks version is set
    run bash -c 'fluvio version > active_flv_ver.out && cat active_flv_ver.out | head -n 1 | grep "0.10.15"'
    assert_output --partial "0.10.15"
    assert_success

    # Switch version to use Fluvio at 0.10.14
    run bash -c 'fvm switch 0.10.14'
    assert_success

    # Checks version is set
    run bash -c 'fluvio version > active_flv_ver.out && cat active_flv_ver.out | head -n 1 | grep "0.10.14"'
    assert_output --partial "0.10.14"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Keeps track of the active Fluvio Version in settings.toml" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Ensure `~/.fluvio` is not present
    run bash -c '! test -d $FLUVIO_HOME_DIR'
    assert_success

    # Installs Fluvio Stable
    run bash -c 'fvm install stable'
    assert_success

    # Installs Fluvio at 0.10.14
    run bash -c 'fvm install 0.10.14'
    assert_success

    # Switch version to use Fluvio at Stable
    run bash -c 'fvm switch stable'
    assert_success

    # Checks channel is set
    run bash -c 'cat ~/.fvm/settings.toml | grep "channel = \"stable\""'
    assert_output --partial "channel = \"stable\""
    assert_success

    # Checks the version is set as active in show list
    run bash -c 'fvm show'
    assert_line --index 0 --partial "    CHANNEL  VERSION"
    assert_line --index 1 --partial " ✓  stable   $STABLE_VERSION"
    assert_line --index 2 --partial "    0.10.14  0.10.14"
    assert_success

    # Checks current command output
    run bash -c 'fvm current'
    assert_line --index 0 "$STABLE_VERSION (stable)"
    assert_success

    # Checks version is set
    run bash -c 'cat ~/.fvm/settings.toml | grep "version = \"$STABLE_VERSION\""'
    assert_output --partial "version = \"$STABLE_VERSION\""
    assert_success

    # Switch version to use Fluvio at 0.10.14
    run bash -c 'fvm switch 0.10.14'
    assert_success

    # Checks version is set
    run bash -c 'cat ~/.fvm/settings.toml | grep "version = \"0.10.14\""'
    assert_output --partial "version = \"0.10.14\""
    assert_success

    # Checks channel is tag
    run bash -c 'cat ~/.fvm/settings.toml | grep "tag = \"0.10.14\""'
    assert_output --partial "tag = \"0.10.14\""
    assert_success

    # Checks the version is set as active in show list
    run bash -c 'fvm show'
    assert_line --index 0 --partial "    CHANNEL  VERSION"
    assert_line --index 1 --partial " ✓  0.10.14  0.10.14"
    assert_line --index 2 --partial "    stable   $STABLE_VERSION"
    assert_success

    # Checks current command output
    run bash -c 'fvm current'
    assert_line --index 0 "0.10.14"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Recommends using fvm show to list available versions" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    run bash -c 'fvm switch'
    assert_line --index 0 "help: You can use fvm show to see installed versions"
    assert_line --index 1 "Error: No version provided"
    assert_failure

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success
}

@test "Supress output with '-q' optional argument" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Expect no output if `-q` is passed
    run bash -c 'fvm -q install stable'
    assert_output ""
    assert_success

    # Expect output if `-q` is not passed
    run bash -c 'fvm install 0.10.14'
    assert_line --index 0 --partial "info: Downloading (1/5)"
    assert_output --partial "done: Now using fluvio version 0.10.14"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Renders help text on current command if none active" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    run bash -c 'fvm current'
    assert_line --index 0 "warn: No active version set"
    assert_line --index 1 "help: You can use fvm switch to set the active version"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Sets the desired version after installing it" {
    export VERSION="0.10.14"

    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    run bash -c 'fvm current'
    assert_line --index 0 "warn: No active version set"
    assert_line --index 1 "help: You can use fvm switch to set the active version"
    assert_success

    run bash -c 'fvm install $VERSION'
    assert_success

    run bash -c 'fvm current'
    assert_line --index 0 "$VERSION"
    assert_success

    run bash -c 'fluvio version > flv_version_$version.out && cat flv_version_$version.out | head -n 1 | grep "$VERSION"'
    assert_output --partial "$VERSION"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Replaces binary on installs" {
    # Checks the presence of the binary in the versions directory
    run bash -c '! test -f "$FVM_HOME_DIR/bin/fvm"'
    assert_success

    # Create FVM Binaries directory
    mkdir -p "$FVM_HOME_DIR/bin"

    # Create bash file to check if the binary is present
    run bash -c 'echo "echo \"Hello World!\"" > "$FVM_HOME_DIR/bin/fvm"'
    assert_success

    # Store this file Sha256 Checksum
    export SHASUM_TEST_FILE=$(sha256sum "$FVM_HOME_DIR/bin/fvm" | awk '{ print $1 }')

    # Install FVM using `self install`
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Store FVM Binary Sha256 Checksum
    export SHASUM_FVM_BIN=$(sha256sum "$FVM_HOME_DIR/bin/fvm" | awk '{ print $1 }')

    # Ensure the checksums are different
    [[ "$SHASUM_TEST_FILE" != "$SHASUM_FVM_BIN" ]]
    assert_success

    # Ensure file is not corrupted on updates
    run bash -c 'fvm --help'
    assert_output --partial "Fluvio Version Manager (FVM)"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success
}

@test "Updating keeps settings files integrity" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    run bash -c 'fvm install stable'
    assert_success

    # Store Settings File Checksum
    export SHASUM_SETTINGS_BEFORE=$(sha256sum "$FVM_HOME_DIR/settings.toml" | awk '{ print $1 }')

    # We cannot use `fvm self install` so use other copy of FVM to test binary
    # replacement
    run bash -c '$FVM_BIN self install'
    assert_success

    # Store Settings File Checksum
    export SHASUM_SETTINGS_AFTER=$(sha256sum "$FVM_HOME_DIR/settings.toml" | awk '{ print $1 }')

    assert_equal "$SHASUM_SETTINGS_BEFORE" "$SHASUM_SETTINGS_AFTER"

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Fails when using 'fvm self install' on itself" {
    skip "Ubuntu CI does not support this test due to mounted virtual volume path mismatch"

    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    run bash -c 'fvm install stable'
    assert_success

    # We cannot use `fvm self install` so use other copy of FVM to test binary
    # replacement
    run bash -c 'fvm self install'
    assert_output --partial "Error: FVM is already installed"
    assert_failure

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}
