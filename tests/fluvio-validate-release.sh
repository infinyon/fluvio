#!/usr/bin/env bash

#set -exu
set -eu

readonly FLUVIO_BIN=~/.fluvio/bin/fluvio
readonly FLUVIO_VERSION_CHECK=${1?Pass in expected version in pos 1}
readonly FLUVIO_COMMIT_CHECK=${2?Pass in expected commit in pos 2}

# This function should always run first
function validate_installer_output() {
    # Validate the installer output returns the expected version
    curl -fsS https://hub.infinyon.cloud/install/install.sh?ctx=ci | bash | tee /tmp/installer.output

    INSTALLED_FLUVIO_VERSION=$(cat /tmp/installer.output | grep "Downloading Fluvio" | awk '{print $5}' | tr -d '[:space:]')
    EXPECTED_FLUVIO_VERSION=$FLUVIO_VERSION_CHECK

    if [ "$INSTALLED_FLUVIO_VERSION" = "$EXPECTED_FLUVIO_VERSION" ]; then
      echo "✅ Version reported by installer: $INSTALLED_FLUVIO_VERSION";
    else
      echo "❌ Version install check failed";
      echo "Version reported by installer: $INSTALLED_FLUVIO_VERSION";
      echo "Expected version: $EXPECTED_FLUVIO_VERSION";
      exit 1;
    fi
}

function validate_fluvio_sha256() {
    # Validate fluvio binary checksum
    INSTALLED_FLUVIO_SHASUM=$($FLUVIO_BIN version | grep SHA256 | awk '{print $5}')
    EXPECTED_FLUVIO_SHASUM=$(shasum -a 256 $FLUVIO_BIN  | awk '{print $1}' | tr -d '[:space:]')

    if [ "$INSTALLED_FLUVIO_SHASUM" = "$EXPECTED_FLUVIO_SHASUM" ]; then
      echo "✅ Sha256 check by fluvio version passed: $INSTALLED_FLUVIO_SHASUM";
    else
      echo "❌ fluvio version reported unexpected shasum";
      echo "Shasum reported by fluvio version: $INSTALLED_FLUVIO_SHASUM";
      echo "Expected fluvio version: $EXPECTED_FLUVIO_SHASUM";
      exit 1;
    fi
}

function validate_fluvio_commit() {
    # Validate fluvio binary commit 
    INSTALLED_FLUVIO_COMMIT=$(fluvio version | grep Commit | awk '{print $4}' | tr -d '[:space:]')
    EXPECTED_FLUVIO_COMMIT=$FLUVIO_COMMIT_CHECK
    if [ "$INSTALLED_FLUVIO_COMMIT" = "$EXPECTED_FLUVIO_COMMIT" ]; then
      echo "✅ Installed fluvio commit check passed: $INSTALLED_FLUVIO_COMMIT";
    else
      echo "❌ Installed fluvio commit check failed";
      echo "Commit reported by fluvio version: $INSTALLED_FLUVIO_COMMIT";
      echo "Expected commit: $EXPECTED_FLUVIO_COMMIT";
      exit 1;
    fi
}

# Validate fluvio-run version in docker image
function validate_docker_image() {
    # Download the docker image
    EXPECTED_FLUVIO_RUN_VERSION=$FLUVIO_VERSION_CHECK

    # Validate that the docker image has the correct Fluvio binaries
    docker pull infinyon/fluvio:$EXPECTED_FLUVIO_RUN_VERSION
    DOCKER_FLUVIO_RUN_VERSION=$(docker run infinyon/fluvio:$EXPECTED_FLUVIO_RUN_VERSION sh -c "/fluvio-run --version" | awk '{print $2}' | tr -d '[:space:]')

    if [ "$DOCKER_FLUVIO_RUN_VERSION" = "$EXPECTED_FLUVIO_RUN_VERSION" ]; then
      echo "✅ Docker fluvio run version check passed: $EXPECTED_FLUVIO_RUN_VERSION";
    else
      echo "❌ Docker fluvio run version check failed";
      echo "Version reported by fluvio-run: $DOCKER_FLUVIO_RUN_VERSION";
      echo "Expected version: $EXPECTED_FLUVIO_RUN_VERSION";
      exit 1;
    fi
}

function main() {
    validate_installer_output;
    validate_fluvio_sha256;
    validate_fluvio_commit;
    validate_docker_image;
}

main;