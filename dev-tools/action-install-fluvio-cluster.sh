#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.
echo "Installing Fluvio Local Cluster"
if [ "$DEVELOPMENT" = "true" ]; then
        if [ "$VERSION" = "latest" ]; then 
                echo "Installing from latest source"
                # Download Fluvio Repo
                # TODO! TEMPORARY; REPLACE ONCE BINARY IS RELEASED
                # Use only in develop version
                pushd /tmp/
                git clone https://github.com/infinyon/fluvio.git
                popd

                pushd /tmp/fluvio
                # Install Fluvio Src
                cargo install --path ./src/cli
                popd
        else
                echo "Release binary is not available; use 'latest'"
        fi

        # Set fluvio minikube context
        fluvio cluster set-minikube-context

        # Install Fluvio System Charts
        fluvio cluster install --sys

        # Run Fluvio Cluster Pre-Install Check
        fluvio cluster check --pre-install

        if [ "$CLUSTER_TYPE" = "local" ]; then
                # Install Local Fluvio Cluster
                fluvio cluster install --rust-log $RUST_LOG --develop --local --spu $SPU_NUMBER
        else
                echo "Currently, only local cluster types are supported"
        fi
else
        echo "Fluvio production version is currently not supported; use development"
fi