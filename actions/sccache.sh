#!/bin/bash
# delete and re-install minikube ready for fluvio
# this defaults to docker and assume you have have sudo access
set -e
MATRIX_OS=${1}
echo "shell matrix.os $MATRIX_OS"