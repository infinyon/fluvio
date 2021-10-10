#!/bin/bash
set -x
brew install hyperkit
brew install minikube
brew install helm
minikube config set memory 16384
minikube start --driver hyperkit --kubernetes-version=1.21.2
kubectl get nodes