#!/bin/bash
kubectl config set-context flv --namespace=flv   --cluster=mycube --user=minikube
kubectl config use-context flv
