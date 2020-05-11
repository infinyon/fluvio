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



## Setting up development env for Minikube Kubernetes

Due to limitation of third party library, we need to apply DNS name for minikube cluster.

First ensure minikube is set up by the following [instruction](https://www.fluvio.io/docs/getting-started/minikube/).

When creating a new minikube cluster,  please specify kubernetes version 1.14x to ensure maximum compatibility as shown below: 

```
minikube start
```

After cluster is created, run following script to setup your local environment:

```
./dev-tools/minikube-mycube.sh
```

This script performs the following tasks:

* adds a new DNS entry ```minikubeCA``` to your /etc/hosts file
* creates a new ```mycube``` context
* points the API server to ```minikubeCA```


## Installing Fluvio system chart

Before you begin, Make sure to install [Helm](https://helm.sh/docs/intro/install/) client appropriate for your environment.  Currently only helm version 3.0+ is supported.  

Then add fluvio helm repo:
```
helm repo add fluvio https://infinyon.github.io/charts
helm repo update
```

Install Fluvio system charts which install storage driver and CRD:
```
helm install fluvio-sys fluvio/fluvio-sys  --set cloud=minikube
```


## Registering Custom SPU

To run development SPU (custom) from your laptop, you must register them.  

To register 3 SPU:
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

## Deploying development docker images to minikube

### Setup 

Install helm binary
```
brew install helm
```
  
Run following script to allow host docker to access minikube docker.  Without it, you can't upload image to minikube.

```
./dev-tools/minikube-docker.sh 
```

Ensure you have setup tunnel so can access SC and SPU from your machine:
```
sudo ./k8-util/minikube-tunnel.sh
```

### Create development docker image

This build local docker image using current branch of the code.
```
make minikube_image
```

## Installing fluvio to minikube

Fluvio CLI can be used to install on minikube
```
target/debug/fluvio install
```

Ensure it is listed
```
helm list
```

## Installing TLS version to minikube

Ensure no other fluvio installation in the name space.  You can uninstall existing fluvio installation by:
```helm uninstall fluvio```

First, generate TLS certificates with domain "fluvio.local".  This will generate necessary certificates and create Kubernetes secrets:

```make create-secret```

Install TLS version of Fluvio
```target/debug/fluvio install --tls```



## Running integration test

Please ensure Fluvio system chart has been installed.

First, build all targets

```
cargo build
```

Run end to end integration test with a single SPU
```
./target/debug/flv-test
```

Run end to end integration test with a multiple SPU.  For example, with 2 SPU
```
./target/debug/flv-test -r 2
```

### Running integration test on Kubernetes

Prerequisite:
* minikube images
* sys chart
* minikube tunnel

```
./target/debug/flv-test -k
```


## Release Process

* Create and switch to release branch
* Ensure integration tests passes
* Bump up VERSION and related crates
* Release docker image: ```make release_image```
* Bump up helm version same as VERSION in the helm/fluvio-core/Chart.yaml
* Generates charts: ```make helm_package```
* Commit and Push chart repo: infinyon.github.io/charts
* Commit and merged/rebase release branch into master
* Generates github releases:
  * ```make create_release```
  * ```make upload_release```




