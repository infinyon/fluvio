#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.
echo "Installing Fluvio Local Cluster"
if [ "$DEVELOPMENT" = "true" ]; then
        case $VERSION in
                "latest")
                        echo "Installing from latest source"
                        # Download Fluvio Repo
                        # TODO! TEMPORARY; REPLACE ONCE BINARY IS RELEASED
                        # Use only in develop version
                        cd /tmp/
                        git clone https://github.com/infinyon/fluvio.git
                        cd -

                        cd /tmp/fluvio
                        # Install Fluvio Src
                        cargo install --path ./src/cli
                        cd -
                ;;
                "v"*)
                        echo "Installing Fluvio $VERSION"
                        cd /tmp/
                        wget https://raw.githubusercontent.com/infinyon/fluvio/feature/alpha_install_script/dev-tools/install-fluvio.sh
                        export OSTYPE=$OSTYPE
                        export SHELL=$SHELL
                        export HOME=$HOME
                        sh ./install-fluvio.sh
                        cd -
                        echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bash_profile
                        . $HOME/.bash_profile
                ;;
                *)
                        echo "Release binary is not available for $VERSION; use 'latest'"
                ;;
        esac

        
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