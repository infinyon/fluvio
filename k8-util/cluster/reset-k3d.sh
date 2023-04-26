#!/bin/bash
# delete and re-install k3d cluster ready for fluvio
# this defaults to docker and assume you have have sudo access
set -e

K8_VERSION=${K8_VERSION:-v1.26.3}

k3d cluster delete fluvio
k3d cluster create fluvio --image rancher/k3s:${K8_VERSION}-k3s1