#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.
echo "Installing Fluvio Local Cluster"
if [ "$DEVELOPMENT" = "true" ]; then
        case $VERSION in
                "latest")
                        echo "Installing from latest source"
                        curl -sSf https://raw.githubusercontent.com.infinyon/fluvio/master/dev-tools/install.sh | bash
                        echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bash_profile
                        . $HOME/.bash_profile
                ;;
                "v"*)
                        echo "Installing Fluvio $VERSION"
                        curl -sSf https://raw.githubusercontent.com.infinyon/fluvio/master/dev-tools/install.sh | bash
                        echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bash_profile
                        . $HOME/.bash_profile
                ;;
                *)
                        echo "Release binary is not available for $VERSION; use 'latest'"
                ;;
        esac

        
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