#!/bin/bash
# delete and re-install minikube ready for fluvio
# it uses docker as default driver
set -e
ARG1=${1:-docker}
K8_VERSION=${2:-1.21.2}
minikube delete
minikube start --driver $ARG1 --kubernetes-version=$K8_VERSION --extra-config=apiserver.service-node-port-range=32700-32800 --ports=127.0.0.1:32700-32800:32700-32800
# minikube start --extra-config=apiserver.v=10