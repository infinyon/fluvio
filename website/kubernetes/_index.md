---
title: Kubernetes Overview
folder: Kubernetes
menu: Overview
weight: 110
---

Fluvio was designed to work natively with Kubernetes. You can deploy Fluvio to Kubernetes using the Helm chart. This section documents the integration steps between Fluvio and Kubernetes.

## K8 Integration Architecture

TODO: Make this section legible:

<a href="https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources" target="_blank">custom resources</a> and <a href="https://kubernetes.io/docs/concepts/extend-kubernetes/operator" target="_blank">operators</a> to integrate **managed SPUs** with Kubernetes. Custom operators utilize <a href="https://kubernetes.io/docs/concepts/workloads/controllers/replicaset" target="_blank">replica sets</a> to govern the number of SPUs that Kubernetes should provision and maintain. Managed SPU provisioned through replica sets cannot be manually modified.

* purpose built operator with custom CRD
* helm chart installation package
* inspect and show all Fluvio objects using K8 commands
* you have the option to use KubeCtl instead of Fluvio CLI to manage a Fluvio environment
* support multiple storage options: Minikube, EKS, Local store
* SPU-Groups leverages K8 stateful sets to scale SPUs
* leverage external load balancer to allow consumers and producers to securely connect to the SC and SPUs
* support multiple installations of Fluvio using K8 namespaces
* Fluvio installer for K8 installation

## K8 Management

Object Reference & Equivalent KubeCtl command

* spu
* spu-group
* topic
* partitions

## Docker Images

Name of Docker Image file available ... in Dockerhub

## Environment Variables 

Explain the environment variables