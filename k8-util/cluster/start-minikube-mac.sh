#!/bin/bash
set -x
brew install minikube
minikube config set memory 16384
minikube start --driver virtualbox --kubernetes-version=1.21.2
kubectl get nodes