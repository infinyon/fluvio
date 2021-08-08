#!/bin/bash
# delete and re-install minikube ready for fluvio
# it uses docker as default driver
set -e
set -x
ARG1=${1:-docker}
K8_VERSION=${2:-1.21.2}

if [ "$(uname)" == "Darwin" ]; then
EXTRA_CONFIG="--extra-config=apiserver.service-node-port-range=32700-32800 --ports=127.0.0.1:32700-32800:32700-32800"
fi


minikube delete
minikube start --driver $ARG1 --kubernetes-version=$K8_VERSION $EXTRA_CONFIG
# minikube start --extra-config=apiserver.v=10