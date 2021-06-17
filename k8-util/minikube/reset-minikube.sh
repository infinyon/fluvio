#!/bin/bash
# delete and re-install minikube ready for fluvio
# this defaults to docker and assume you have have sudo access
set -e
ARG1=${1:-docker}
minikube delete
minikube start --driver $ARG1 --kubernetes-version 1.19.6