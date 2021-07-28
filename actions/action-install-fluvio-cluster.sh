#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.

set -eu -o pipefail
echo "Installing Fluvio Local Cluster"

curl -fsS https://packages.fluvio.io/v1/install.sh | bash
echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bash_profile
. $HOME/.bash_profile


LOCAL_FLAG=""
IMAGE=""
#
# Install Fluvio Cluster
#

# Install Local Fluvio Cluster
if [ "$CLUSTER_TYPE" = "local" ]; then
    LOCAL_FLAG="--local"
fi

# For latest, we need to put image tag
if [ "$VERSION" = "latest" ]; then
    IMAGE="--image-version latest"
fi


fluvio cluster start $IMAGE --rust-log $RUST_LOG  $LOCAL_FLAG --spu $SPU_NUMBER