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

Add standard library for the target platform:

```
rustup target add x86_64-unknown-linux-musl
```
To build a full cross compiler toolchain targeting musl Linux amd64, perform:

```
brew install filosottile/musl-cross/musl-cross
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
```
 docker run -d -p 5000:5000 --restart=always --name registry registry:2
```
Set the following environment variable:

```
export TARGET_CC=x86_64-linux-musl-gcc
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

Note that when you uninstall cluster, CLI will remove all related objects such as
* Topics
* Partitions
* Tls Secrets
* Storage


# Running Fluvio using custom SPU

There are 2 types of SPU supported.  Default is managed SPU which are running in Kubernetes Cluster.  Second is "custom" SPU which  can be run outside Kubernetes.  This can be useful for develop and test SPU in your local laptop.  

It is recommended to use custom SPU when you are working on feature development.


## Creating local cluster

Local cluster of custom SPU can be created same manner previously:

```
flvd cluster install --local --spu <spu>
```

where ```---spu``` is optional.  This will launch SC and SPU's using native build instead of docker images.

The logs for SC and SPU can be found in:
* /tmp/flv_sc.log
* /tmp/spu_log_<spu_id>.og


## Uninstalling local cluster

Local cluster can be uninstalled as:

```
flvd cluster uninstall --local
```


## Troubleshooting
This guide helps users to solve issues they might face during the setup process. 

###### Cross-compilation errors

If you face cross-compilation errors while creating minikube image, for example

```
cargo build --bin spu-server --target x86_64-unknown-linux-musl
error: linker `x86_64-linux-musl-gcc` not found
 |
 = note: No such file or directory (os error 2)
error: aborting due to previous error
error: could not compile `flv-spu`.
```
This is indicative that you need to add standard library for the target platform:

```
rustup target add x86_64-unknown-linux-musl
```

If it still doesn't work

```
brew install filosottile/musl-cross/musl-cross
```

Make sure you set the following environment variable

```
export TARGET_CC=x86_64-linux-musl-gcc
```

###### Connection issues

If you face issues while connecting to the registry

```
Get http://localhost:5000/v2/: dial tcp [::1]:5000: connect: connection refused
```

It means your docker registry is not running

```
docker run -d -p 5000:5000 --restart=always --name registry registry:2
```
