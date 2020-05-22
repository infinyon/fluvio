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

Currently,  Fluvio is using the nightly version of Rust because it is using unstable version of the Futures library.  Currently we are depending on following nightly features:

- [Default specialization](https://github.com/rust-lang/rust/issues/37653)


# Fluvio cluster
Fluvio is installed as cluster of components designed to work in Kubernetes:

## Streaming Controller (SC)
Streaming Controller implements control plane.  It is responsible for organizing and coordinating data streams between SPU's.  It uses the declarative model to self-heal and recover much as possible during failures.

## Streaming Processing Engine (SPU)
SPU's are engine for processing streams.   Each SPU can handle multiple data streams.   SPU uses reactive and asynchronous architecture to ensure efficient handling of data. 

## Fluvio CLI(Command Line Interface

CLI provides built-in way to manage Fluvio streams and interaction.  It can manage
* Topics
* SPU and SPU group
* Consume and produce messages to streams


# Building Fluvio

## Set up Rust

Please follow [setup](https://www.rust-lang.org/tools/install) instructions to install Rust and Cargo.

## Checkout and build

This will build and run unit tests forFluvio for your environment:

```
$ git clone https://github.com/infinyon/fluvio.git
$ cd fluvio
$ cargo build
$ cargo test
```

You can run development version of fluvio CLI:
```
$ target/debug/fluvio
```

You can assign alias to simplify references to CLI like this:
```
alias flvd=target/debug/fluvio
```

From now on, we will reference ```flvd``` instead of release version.

## Setting up Kubernetes Clusters and Installing system chart

Please follow instruction on INSTALL.md for setting up kubernetes clusters and installing fluvio system chart.


## Deploying development version of Fluvio cluster to Kubernetes

Please ensure local docker registry is running:

```
./dev-tools/minikube-docker.sh 
```

Then build docker images for current source code:
```
make minikube_image
```

You can install develop version of fluvio using same installation command:
```
flvd cluster install --develop
```

You can remove fluvio cluster by
```
flvd cluster uninstall
```


# Running Fluvio using custom SPU

There are 2 types of SPU supported.  Default is managed SPU which are running in Kubernetes Cluster.  Second is "custom" SPU which  can be run outside Kubernetes.  This can be useful for develop and test SPU in your local laptop.  

It is recommended to use custom SPU when you are working on feature development.


## Registering Custom SPU

To run custom SPU (custom) from your laptop, you must register their configuration.  Using sample configuration, you can register a SPU with id of 5001

```
kubectl create -f k8-util/samples/crd/spu_5001.yaml
```  

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

To start a single SPU using configuration registered as above:

```
./dev-tools/log/debug-spu-min 5001 9005 9006
```

To start 3 SPU with in separate terminal session:
```
./dev-tools/log/debug-spu-min 5002 9007 9008
```
```
./dev-tools/log/debug-spu-min 5003 9009 9010
```

## Using CLI

To connect to custom SPU, create local profile:
```
flvd profile create-local-profile
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

### Running integration test for custom SPU


```
./target/debug/flv-test --custom
```



