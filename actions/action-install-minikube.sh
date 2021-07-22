#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.

set -eu -o pipefail
function error_msg_unsupported_os {
    echo "unsupported operating system; ignoring minikube install"
    exit 1
}

echo "Installing Minikube"
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "Installing for Linux"
    PLATFORM=linux-amd64
    # Install Minikube for ubuntu
    # Pre-Installation Check
    # grep -E --color 'vmx|svm' /proc/cpuinfo

    echo "Installing Minikube"
    # Install Minikube
    curl -Lo minikube https://storage.googleapis.com/minikube/releases/${MINIKUBE_VERSION}/minikube-${PLATFORM} \
    && chmod +x minikube
    sudo install minikube /usr/local/bin/

    # Install conntrack (required for the none driver)
    sudo apt install conntrack

    # Start Minikube with `none` driver
    minikube start --driver=none --kubernetes-version $K8_VERSION

    # Update permissions for .kube and .minikube
    sudo chown -R $USER $HOME/.kube $HOME/.minikube


elif [[ "$OSTYPE" == "darwin"* ]]; then
    # Mac OSX
    PLATFORM=darwin-amd64

    brew install minikube


elif [[ "$OSTYPE" == "cygwin" ]]; then
    # POSIX compatibility layer and Linux environment emulation for Windows
    error_msg_unsupported_os
elif [[ "$OSTYPE" == "msys" ]]; then
    # Lightweight shell and GNU utilities compiled for Windows (part of MinGW)
    error_msg_unsupported_os
elif [[ "$OSTYPE" == "win32" ]]; then
    # I'm not sure this can happen.
    error_msg_unsupported_os
elif [[ "$OSTYPE" == "freebsd"* ]]; then
    # ...
    error_msg_unsupported_os
else
    # Unknown.
    error_msg_unsupported_os
fi
