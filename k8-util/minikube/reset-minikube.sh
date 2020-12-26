#!/bin/bash
# delete and re-install minikube ready for fluvio
# this defaults to docker and assume you have have sudo access
set -e
ARG1=${1:-docker}
minikube delete
minikube start --driver $ARG1
sudo nohup  minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out &
fluvio cluster start --sys