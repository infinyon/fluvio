#!/bin/bash
# delete and re-install minikube ready for fluvio
# this defaults to docker and assume you have have sudo access
set -e
ARG1=${1:-docker}
K8_VERSION=${2:-1.21.2}
minikube delete
minikube start --driver $ARG1 --kubernetes-version=$K8_VERSION