#!/bin/bash
# delete and re-install k3d cluster ready for fluvio
# this defaults to docker and assume you have have sudo access
set -e
k3d cluster delete fluvio
k3d cluster create fluvio