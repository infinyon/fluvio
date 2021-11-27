#!/bin/bash
#
# Hyperkit doesn't work for M1
# It also is not reliable in Github Actions
# Only reliable way to run is use of kind in Docker
set -x
brew install minikube
brew install hyperkit
minikube config set memory 1024
minikube start --driver=hyperkit
kubectl get nodes