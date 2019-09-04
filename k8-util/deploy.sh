#!/bin/bash
mydir="$(dirname "${0}")"
kubectl apply -f ${mydir}/sc-deployment/sc-deployment.yaml
kubectl apply -f ${mydir}/sc-deployment/sc-internal.yaml 
kubectl apply -f ${mydir}/sc-deployment/sc-public.yaml 

