#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.

set -eu -o pipefail
echo "Installing Fluvio Local Cluster"

curl -fsS https://hub.infinyon.cloud/install/install.sh?ctx=ci | bash
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
# Install K8S Fluvio Cluster
if [ "$CLUSTER_TYPE" = "k8" ]; then
    LOCAL_FLAG="--k8"
fi

# For latest, we need to put image tag
if [ "$VERSION" = "latest" ]; then
    IMAGE="--image-version latest"
fi


fluvio cluster start $IMAGE --rust-log $RUST_LOG  $LOCAL_FLAG --spu $SPU_NUMBER
