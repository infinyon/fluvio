#!/usr/bin/env bats

TEST_HELPER_DIR="$BATS_TEST_DIRNAME/../test_helper"
export TEST_HELPER_DIR

load "$TEST_HELPER_DIR"/tools_check.bash
load "$TEST_HELPER_DIR"/fluvio_dev.bash
load "$TEST_HELPER_DIR"/bats-support/load.bash
load "$TEST_HELPER_DIR"/bats-assert/load.bash

setup_file() {
    # Tests in this file are executed in order and rely on the previous test
    # to be successful.

    INFINYON_HUB_REMOTE="https://hub.infinyon.cloud"
    export INFINYON_HUB_REMOTE
    debug_msg "Using Hub Registry URL: $INFINYON_HUB_REMOTE"

    export INFINYON_CI_CONTEXT="ci"

    # Retrieves the latest stable version from the GitHub API and removes the
    # `v` prefix from the tag name.
    STABLE_VERSION=$(curl "https://api.github.com/repos/infinyon/fluvio/releases/latest" | jq -r .tag_name | cut -c2-)
    export STABLE_VERSION
    debug_msg "Stable Version: $STABLE_VERSION"

    # Fetches current Fluvio Cloud CLI Stable Version
    FLUVIO_CLOUD_STABLE_VERSION=$(curl "https://packages.fluvio.io/v1/packages/fluvio/fluvio-cloud/tags/stable")
    export FLUVIO_CLOUD_STABLE_VERSION
    debug_msg "Fluvio Cloud Stable Version: $FLUVIO_CLOUD_STABLE_VERSION"

    # Fetches current FVM Stable Version
    FVM_VERSION_STABLE=$(curl "https://packages.fluvio.io/v1/packages/fluvio/fvm/tags/stable")
    export FVM_VERSION_STABLE
    debug_msg "FVM Stable Version: $FVM_VERSION_STABLE"

    # Fetches current FVM Stable Version Sha256
    FVM_VERSION_STABLE_SHA256=$(curl "https://packages.fluvio.io/v1/packages/fluvio/fvm/$FVM_VERSION_STABLE/x86_64-unknown-linux-musl/fvm.sha256")
    export FVM_VERSION_STABLE_SHA256
    debug_msg "FVM Stable Version Sha256: $FVM_VERSION_STABLE_SHA256"

    FVM_UPDATE_CUSTOM_VERSION="0.11.0"
    export FVM_UPDATE_CUSTOM_VERSION
    debug_msg "FVM Update Custom Version: $FVM_UPDATE_CUSTOM_VERSION"

    FVM_UPDATE_CUSTOM_VERSION_SHA256=$(curl "https://packages.fluvio.io/v1/packages/fluvio/fvm/$FVM_UPDATE_CUSTOM_VERSION/x86_64-unknown-linux-musl/fvm.sha256")
    export FVM_UPDATE_CUSTOM_VERSION_SHA256
    debug_msg "FVM Update Custom Version Sha256: $FVM_UPDATE_CUSTOM_VERSION_SHA256"

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

    VERSION_FILE="$(cat ./VERSION)"
    export VERSION_FILE
    debug_msg "Version File Value: $VERSION_FILE"
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

    # Renders warn if attempts to use `fvm list` and no versions are installed
    run bash -c 'fvm list'
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

        # Sleeps to avoid hiting rate limits
        sleep 30

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

        # Sleeps to avoid hiting rate limits
        sleep 30
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

    # Sleeps to avoid hiting rate limits
    sleep 30

    # Installs Fluvio at 0.10.14
    run bash -c 'fvm install 0.10.14'
    assert_success

    # Sleeps to avoid hiting rate limits
    sleep 30

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

    # Sleeps to avoid hiting rate limits
    sleep 30

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

    # Checks the version is set as active in list list
    run bash -c 'fvm list'
    assert_line --index 0 --partial "    CHANNEL  VERSION"
    assert_line --index 1 --partial " ✓  stable   $STABLE_VERSION"
    assert_line --index 2 --partial "    0.10.14  0.10.14"
    assert_success

    # Checks contents for the stable channel
    run bash -c 'fvm list stable'
    assert_line --index 0 --partial "Artifacts in channel stable version $STABLE_VERSION"
    assert_line --index 1 --partial "fluvio@$STABLE_VERSION"
    assert_success

    # Checks contents for the version 0.10.14
    run bash -c 'fvm list 0.10.14'
    assert_line --index 0 --partial "Artifacts in version 0.10.14"
    assert_line --index 1 --partial "fluvio@0.10.14"
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

    # Checks the version is set as active in list
    run bash -c 'fvm list'
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

@test "Recommends using fvm list to list available versions" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    run bash -c 'fvm switch'
    assert_line --index 0 "help: You can use fvm list to see installed versions"
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

    # Sleeps to avoid hiting rate limits
    sleep 30

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

@test "Prints version with details on fvm version" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    run bash -c 'fvm version'
    assert_line --index 0 "fvm CLI: $VERSION_FILE"
    assert_line --index 1 --partial "fvm CLI Arch: "
    assert_line --index 2 --partial "fvm CLI SHA256: "
    assert_line --index 3 --partial "OS Details: "
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Updates version in channel" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Installs the stable version
    run bash -c 'fvm install stable'
    assert_success

    # Changes the version set as `stable` channel to $STATIC_VERSION in order to
    # force an update. $STATIC_VERSION just to reuse the variable to improve
    # readability, it could be any version tag (but stable of course!)
    #
    # This wont work in macOS because the sed command is different there
    sed -i "s/$STABLE_VERSION/$STATIC_VERSION/g" $FVM_HOME_DIR/settings.toml

    # Checks active version
    run bash -c 'fvm current'
    assert_line --index 0 "$STATIC_VERSION (stable)"
    assert_success

    # Attempts to update Fluvio
    run bash -c 'fvm update'
    assert_line --index 0 "info: Updating fluvio stable to version $STABLE_VERSION. Current version is $STATIC_VERSION."
    assert_success

    # Checks active version
    run bash -c 'fvm current'
    assert_line --index 0 "$STABLE_VERSION (stable)"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Do not updates version in static tag" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Installs the stable version
    run bash -c 'fvm install $STATIC_VERSION'
    assert_success

    # Attempts to update Fluvio
    run bash -c 'fvm update'
    assert_line --index 0 "info: Cannot update a static version tag. You must use a channel."
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Renders message when already up-to-date" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Installs the stable version
    run bash -c 'fvm install'
    assert_success

    # Sleeps to avoid hiting rate limits
    sleep 30

    # Attempts to update Fluvio
    run bash -c 'fvm update'
    assert_line --index 0 "done: You are already up to date"
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Handles unexistent version error" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Attempts to install unexistent version
    run bash -c 'fvm install 0.0.0'
    assert_line --index 0 "Error: PackageSet 0.0.0 doest not exist"
    assert_failure

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Uninstall Versions" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Ensure `~/.fvm/versions/stable` is not present
    run bash -c '! test -d $FVM_HOME_DIR/versions/stable'
    assert_success

    # Install stable version
    run bash -c 'fvm install stable'
    assert_success

    # Ensure `~/.fvm/versions/stable` is present
    run bash -c 'test -d $FVM_HOME_DIR/versions/stable'
    assert_success

    # Uninstall stable version
    run bash -c 'fvm uninstall stable'
    assert_success

    # Ensure `~/.fvm/versions/stable` is present
    run bash -c '! test -d $FVM_HOME_DIR/versions/stable'
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Updates artifacts in the stable channel" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Install stable version
    run bash -c 'fvm install stable'
    assert_success

    # Ensure `~/.fvm/versions/stable` is present
    run bash -c 'test -d $FVM_HOME_DIR/versions/stable'
    assert_success

    # Checks for updates
    run bash -c 'fvm update'
    assert_line --index 0 "done: You are already up to date"
    assert_success

    # Updates manifest to trigger update
    sed -i -e "s/$FLUVIO_CLOUD_STABLE_VERSION/0.2.21/g" $FVM_HOME_DIR/versions/stable/manifest.json

    # Removes current `fluvio-cloud` binary so we check it is re-downloaded
    rm -rf $FVM_HOME_DIR/versions/stable/fluvio-cloud

    # Ensure `~/.fvm/versions/stable/fluvio-cloud` IS NOT present
    run bash -c '! test -f $FVM_HOME_DIR/versions/stable/fluvio-cloud'
    assert_success

    # Downloads the update
    run bash -c 'fvm update'
    assert_line --index 0 "info: Found 1 packages in this version that needs update."
    assert_line --index 1 "info: Downloading (1/1): fluvio-cloud@$FLUVIO_CLOUD_STABLE_VERSION"
    assert_line --index 2 "info: Updated fluvio-cloud from 0.2.21 to $FLUVIO_CLOUD_STABLE_VERSION"
    assert_success

    # Ensure `~/.fvm/versions/stable/fluvio-cloud` IS present
    run bash -c 'test -f $FVM_HOME_DIR/versions/stable/fluvio-cloud'
    assert_success

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Updates FVM" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Checks installed version
    run bash -c 'fvm version'
    assert_line --index 0 "fvm CLI: $VERSION_FILE"
    assert_success

    # Store this file Sha256 Checksum
    export CURR_FVM_BIN_CHECKSUM=$(sha256sum "$FVM_HOME_DIR/bin/fvm" | awk '{ print $1 }')

    # Store FVM_BIN file Sha256 Checksum
    export FVM_BIN_CHECKSUM=$(sha256sum "$FVM_BIN" | awk '{ print $1 }')

    # Ensure the installed FVM matches test binary FVM
    [[ "$CURR_FVM_BIN_CHECKSUM" == "$FVM_BIN_CHECKSUM" ]]
    assert_success

    if [[ "$FVM_VERSION_STABLE" = "$VERSION_FILE" ]]; then
        # Updates FVM
        run bash -c 'fvm self update'
        assert_line --index 0 "info: Already up-to-date"
        assert_success
    else
        # Updates FVM
        run bash -c 'fvm self update'
        assert_line --index 0 "info: Updating FVM from $VERSION_FILE to $FVM_VERSION_STABLE"
        assert_line --index 1 "info: Downloading fvm@$FVM_VERSION_STABLE"
        assert_line --index 2 "info: Installing fvm@$FVM_VERSION_STABLE"
        assert_line --index 3 "done: Installed fvm@$FVM_VERSION_STABLE with success"
        assert_success

        # Store FVM Binary Sha256 Checksum
        export NEXT_FVM_BIN_CHECKSUM=$(sha256sum "$FVM_HOME_DIR/bin/fvm" | awk '{ print $1 }')

        # Ensure the checksums matches upstream FVM checksum for this architecture
        run bash -c "[[ "$FVM_VERSION_STABLE_SHA256" == "$NEXT_FVM_BIN_CHECKSUM" ]]"
        assert_success
    fi

    # Removes FVM
    run bash -c 'fvm self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Installs Custom FVM Version on FVM Update" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Checks installed version
    run bash -c '$FVM_BIN version'
    assert_line --index 0 "fvm CLI: $VERSION_FILE"
    assert_success

    # Updates FVM using FVM as of Fluvio 0.11.0
    run bash -c "FVM_UPDATE_VERSION=$FVM_UPDATE_CUSTOM_VERSION $FVM_BIN self update"
    assert_line --index 0 "info: Updating FVM from $VERSION_FILE to $FVM_UPDATE_CUSTOM_VERSION"
    assert_success

    # Store FVM Binary Sha256 Checksum
    export NEXT_FVM_BIN_CHECKSUM=$(sha256sum "$FVM_HOME_DIR/bin/fvm" | awk '{ print $1 }')

    # Ensure the checksums matches upstream FVM checksum for this architecture
    run bash -c "[[ "$FVM_UPDATE_CUSTOM_VERSION_SHA256" == "$NEXT_FVM_BIN_CHECKSUM" ]]"
    assert_success

    # Removes FVM
    run bash -c '$FVM_BIN self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}

@test "Supports Binary Target Overriding" {
    run bash -c '$FVM_BIN self install'
    assert_success

    # Sets `fvm` in the PATH using the "env" file included in the installation
    source ~/.fvm/env

    # Attempts to install unsupported target triple
    run bash -c '$FVM_BIN install 0.11.12 --target aarch64-unknown-linux-gnu'
    assert_line --index 0 "Error: PackageSet \"0.11.12\" is not available for architecture: \"aarch64-unknown-linux-gnu\""
    assert_failure

    # Removes FVM
    run bash -c '$FVM_BIN self uninstall --yes'
    assert_success

    # Removes Fluvio
    rm -rf $FLUVIO_HOME_DIR
    assert_success
}
