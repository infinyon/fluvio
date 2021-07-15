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

#
# Install Fluvio System Charts
#

# If VERSION is equal to exactly "latest", use LATEST channel
if [ "${VERSION}" == "latest" ]; then
    fluvio cluster start --setup --local --sys --chart-version="${REPO_VERSION}-c963500f9d985a1a42b67380bf7bb683cdace1d7"
#    fluvio cluster start --setup --local --sys --chart-version="${CHART_VERSION}"
else
    fluvio cluster start --setup --local --sys
fi

#
# Run Fluvio Cluster Pre-Install Check
#

fluvio cluster check

# Install Local Fluvio Cluster
if [ "$CLUSTER_TYPE" = "local" ]; then

    # If VERSION is equal to exactly "latest", use LATEST channel
    if [ "${VERSION}" == "latest" ]; then
        fluvio cluster start --rust-log $RUST_LOG --develop --local --spu $SPU_NUMBER --chart-version="${REPO_VERSION}-c963500f9d985a1a42b67380bf7bb683cdace1d7"
#        fluvio cluster start --rust-log $RUST_LOG --develop --local --spu $SPU_NUMBER --chart-version="${CHART_VERSION}"
    else
        fluvio cluster start --rust-log $RUST_LOG --develop --local --spu $SPU_NUMBER
    fi
else
    echo "Currently, only local cluster types are supported"
fi
