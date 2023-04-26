#!/bin/bash
# delete and re-install minikube ready for fluvio
# it uses docker as default driver
set -e
set -x
K8_VERSION=${K8_VERSION:-v1.26.3}

# set up default driver, use hyperkit for mac
if [ "$(uname)" == "Darwin" ]; then

DRIVER=${DRIVER:-hyperkit}
echo "Using driver: $DRIVER"
# for mac, if driver is docker, set up proxy
if [ ${DRIVER} == "docker" ]; then
EXTRA_CONFIG="--extra-config=apiserver.service-node-port-range=32700-32800 --ports=127.0.0.1:32700-32800:32700-32800"
fi

else 
DRIVER=${DRIVER:-docker}
fi


minikube delete
minikube start --driver $DRIVER --kubernetes-version=$K8_VERSION $EXTRA_CONFIG
# minikube start --extra-config=apiserver.v=10