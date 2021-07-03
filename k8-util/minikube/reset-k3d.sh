#!/bin/bash
# delete and re-install minikube ready for fluvio
# this defaults to docker and assume you have have sudo access
set -e
ARG1=${1:-docker}
k3d cluster delete fluvio
k3d cluster create fluvio --image rancher/k3s:v1.19.12-k3s1-amd64