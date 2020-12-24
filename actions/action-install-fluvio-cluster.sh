#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.

set -eu -o pipefail
echo "Installing Fluvio Local Cluster"

echo "Installing Fluvio CLI from latest source"
curl -sSf https://raw.githubusercontent.com/infinyon/fluvio/master/install.sh | bash
echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bash_profile
. $HOME/.bash_profile

fluvio cluster check

if [ "$CLUSTER_TYPE" = "local" ]; then
    # Install Fluvio System Charts
    fluvio cluster start --setup --local --sys
    # Install Local Fluvio Cluster
    fluvio cluster start --rust-log $RUST_LOG --develop --local --spu $SPU_NUMBER

    # Run Fluvio Cluster Pre-Install Check

else
    fluvio cluster start --sys
    # Install Local Fluvio Cluster
    fluvio cluster start --rust-log $RUST_LOG --spu $SPU_NUMBER
fi
