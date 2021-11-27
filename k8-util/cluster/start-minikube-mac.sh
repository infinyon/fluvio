#!/bin/bash
set -x
brew install minikube
brew install hyperkit
minikube start --driver=hyperkit
kubectl get nodes