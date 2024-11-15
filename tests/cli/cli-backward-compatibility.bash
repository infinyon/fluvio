#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Set the repository owner and name
OWNER="infinyon"
REPO="fluvio"

# The oldest version we want to test backward compatibility with
BACKWARD_SINCE_VERSION="0.11.10"

# Fetch releases using GitHub API
response=$(curl -s "https://api.github.com/repos/$OWNER/$REPO/releases")

# Check if the response is empty or an error occurred
if [ -z "$response" ]; then
    echo "Failed to fetch releases."
    exit 1
fi

# Filter only non-prerelease releases
versions=$(echo "$response" | jq -r '.[] | select(.prerelease == false) | .tag_name')

for version in $versions; do
    # remove v prefix
    version=$(echo $version | sed 's/v//')
    echo "Running tests for version: $version"

    # Install current version on the cluster
    $FLUVIO_BIN cluster start
    # Install old version of the CLI
    curl -fsS https://hub.infinyon.cloud/install/install.sh?ctx=ci | VERSION=$version bash
    # Run the tests
    CLI_VERSION=$version SKIP_SETUP=true make cli-platform-cross-version-test

    # Check if we have reached the version we want to stop at
    if [[ $version == $BACKWARD_SINCE_VERSION ]]; then
	break
    fi
done
