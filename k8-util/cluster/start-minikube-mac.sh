#!/bin/bash
set -x
brew install --cask virtualbox
brew install minikube
minikube start --driver virtualbox --kubernetes-version=1.21.2
kubect get nodes