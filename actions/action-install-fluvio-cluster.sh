#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.

set -eu -o pipefail
echo "Installing Fluvio Local Cluster"

echo "Installing Fluvio CLI from latest source"
curl -sSf https://raw.githubusercontent.com/infinyon/fluvio/master/install.sh | bash
echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bash_profile
. $HOME/.bash_profile

#
# Install Fluvio Cluster
#

# Install Local Fluvio Cluster
LOCAL_FLAG=""
if [ "$CLUSTER_TYPE" = "local" ]; then
    LOCAL_FLAG="--local"
fi

fluvio cluster start --rust-log $RUST_LOG  $LOCAL_FLAG  --spu $SPU_NUMBER
