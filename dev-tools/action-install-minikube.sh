#!/bin/bash
# This script is ran by the github actions to install fluvio in
# GitHub Action Workflows.
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

        # Install conntrack
        sudo apt install conntrack

        # Start Minikube with `none` driver
        sudo minikube start --driver=none -p minikube

        # Update permissions for .kube and .minikube
        sudo chown -R $USER $HOME/.kube $HOME/.minikube

        # Run Minikube Tunnel
        sudo nohup minikube tunnel >/tmp/tunnel.out 2>/tmp/tunnel.out &

elif [[ "$OSTYPE" == "darwin"* ]]; then
        # Mac OSX
        PLATFORM=darwin-amd64
        echo "unsupported operating system; ignoring fluvio install"
elif [[ "$OSTYPE" == "cygwin" ]]; then
        # POSIX compatibility layer and Linux environment emulation for Windows
        echo "unsupported operating system; ignoring fluvio install"
elif [[ "$OSTYPE" == "msys" ]]; then
        # Lightweight shell and GNU utilities compiled for Windows (part of MinGW)
        echo "unsupported operating system; ignoring fluvio install"
elif [[ "$OSTYPE" == "win32" ]]; then
        # I'm not sure this can happen.
        echo "unsupported operating system; ignoring fluvio install"
elif [[ "$OSTYPE" == "freebsd"* ]]; then
        # ...
        echo "unsupported operating system; ignoring fluvio install"
else
        # Unknown.
        echo "unsupported operating system; ignoring fluvio install"
fi