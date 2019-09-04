#!/bin/bash
mydir="$(dirname "${0}")"
kubectl create clusterrolebinding cluster-system-anonymous --clusterrole=cluster-admin --user=system:anonymous
kubectl apply -f ${mydir}/crd/config/minikube-storageclass-spu.yaml
${mydir}/crd/init.sh
