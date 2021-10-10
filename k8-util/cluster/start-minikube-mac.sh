#!/bin/bash
set -x
brew install minikube
brew install hyperkit
minikube config set memory 16384
minikube start --driver hyperkit --kubernetes-version=1.21.2
kubectl get nodes