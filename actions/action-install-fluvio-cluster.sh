#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.

set -eu -o pipefail
echo "Installing Fluvio Local Cluster"

echo "Installing Fluvio CLI from latest source"
curl -sSf https://raw.githubusercontent.com/infinyon/fluvio/master/install.sh | bash
echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bash_profile
. $HOME/.bash_profile

REPO_VERSION="$(curl -sSf https://raw.githubusercontent.com/infinyon/fluvio/master/VERSION)"
CHART_VERSION="${REPO_VERSION}-${GITHUB_SHA}"

LOCAL_FLAG=""
#
# Install Fluvio Cluster
#

# Install Local Fluvio Cluster
if [ "$CLUSTER_TYPE" = "local" ]; then
    LOCAL_FLAG="--local"
fi

# If VERSION is equal to exactly "latest", use LATEST channel
if [ "${VERSION}" == "latest" ]; then
    fluvio cluster start --rust-log $RUST_LOG  $LOCAL_FLAG --spu $SPU_NUMBER --chart-version="${CHART_VERSION}"
else
    fluvio cluster start --rust-log $RUST_LOG  $LOCAL_FLAG  --spu $SPU_NUMBER
fi
