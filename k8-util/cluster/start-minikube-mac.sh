#!/bin/bash
set -x
brew install minikube
brew install hyperkit
minikube config set memory 16384
minikube start --driver=hyperkit
kubectl get nodes