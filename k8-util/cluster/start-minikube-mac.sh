#!/bin/bash
set -x
brew install minikube
minikube config set memory 1024
minikube start --driver virtualbox
kubectl get nodes