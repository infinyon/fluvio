#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.

set -eu -o pipefail
echo "Installing Fluvio Local Cluster"

echo "Installing Fluvio CLI from latest source"
curl -sSf https://raw.githubusercontent.com/infinyon/fluvio/master/install.sh | bash
echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bash_profile
. $HOME/.bash_profile

# Install Fluvio System Charts
fluvio cluster start --setup --local

# Run Fluvio Cluster Pre-Install Check

fluvio cluster check

if [ "$CLUSTER_TYPE" = "local" ]; then
        # Install Local Fluvio Cluster
        fluvio cluster start --rust-log $RUST_LOG --develop --local --spu $SPU_NUMBER
else
        echo "Currently, only local cluster types are supported"
fi
