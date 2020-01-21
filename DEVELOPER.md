# Fluvio for Developers

Thank you for joining Fluvio community.  The goal of this document is to provide everything you need to get started with developing Fluvio.

## Assumptions

Familiarity with
- [Rust](https://www.rust-lang.org)
- [Kubernetes](https://kubernetes.io)

Developer guide and examples should work with the following platforms:
- macOS X
- Linux
Other platforms such as Windows can be made to work, but we haven't tried them yet.

To test and run services,  you need to get access to development Kubernetes cluster.  Our guide uses Minikube as examples because it is easy to it get it started, but you can use other Kubernetes cluster as well.  Please see  [Kubernetes](https://kubernetes.io) for setting up a development cluster.

# Rust futures and nightly

Currently,  Fluvio is using the nightly version of Rust because it is using unstable version of the Futures library.  We expect to switch to the stable version of Rust in [1.39](https://github.com/rust-lang/rust/pull/63209)


# Fluvio components
Fluvio platform consists of the following components.  

## Streaming Controller (SC)
Streaming Controller implements control plane operations for data-in-motion.  It is responsible for organizing and coordinating data streams between SPU's.  It uses the declarative model to self-heal and recover much as possible during failures.

## Streaming Processing Engine (SPU)
SPU's are the engine for data-in-motion.   Each SPU can handle multiple data streams.   SPU uses reactive and asynchronous architecture to ensure efficient handling of data. 

## CLI
Fluvio CLI provides
manages SPU
manages streams (topics and partitions)
produce and consume streams


# Building Fluvio

## Set up Rust

Please follow [setup](https://www.rust-lang.org/tools/install) instructions to install Rust and Cargo.

## Checkout and build

This will build Fluvio for your environment:

```
$ git clone https://github.com/infinyon/fluvio.git
$ cd fluvio
$ cargo build
$ cargo test
```

# Running SC and SPU in development mode

It is recommended to use custom SPU instead of managed SPU which allow SPU to run locally in your local machine.



## Setting up development env for Minikube

Due to limitation of third party library, we need to apply DNS name for minikube cluster.

First ensure minikube is set up by the following [instruction](https://www.fluvio.io/docs/getting-started/minikube/).

When creating a new minikube cluster,  please specify kubernetes version 1.14x to ensure maximum compatibility as shown below: 

```
minikube start  --kubernetes-version v1.14.9
```

After cluster is created, run following script to setup your local environment:

```
./dev-tools/minikube-mycube.sh
```

This script performs the following tasks:

* adds a new DNS entry ```minikubeCA``` to your /etc/hosts file
* creates a new ```mycube``` context
* points the API server to ```minikubeCA```


## Set up Fluvio CRD

Run script below to install Fluvio CRDs to your cluster:

```
./k8-util/install.sh
```

## Registering Custom SPU

In order to run custom spu, we must register them.  To register 3 SPU:
```
kubectl create -f k8-util/samples/crd/spu_5001.yaml 
kubectl create -f k8-util/samples/crd/spu_5002.yaml 
kubectl create -f k8-util/samples/crd/spu_5003.yaml 
```

## Starting SC
```
./dev-tools/log/debug-sc-min
```

## Starting custom SPU
```
./dev-tools/log/debug-spu-min 5001 9005 9006
./dev-tools/log/debug-spu-min 5002 9007 9008
./dev-tools/log/debug-spu-min 5003 9009 9010
```

## Running CLI

To connect with local SC, use "--sc" parameter as shown below.
Please refer to fluvio web site for CLI operation.

Get SPU

```
 fluvio spu list --sc 127.0.0.1:9003
```

## Setting up for Kubernetes local development

Run following script to allow host docker to access minikube docker.  Without it, you can't upload image to minikube.

```
./dev-tools/minikube-docker.sh 
```









