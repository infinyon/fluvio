# Fluvio Developer Guide

Table of contents:
1. [Setting up Development Environment](#setting-up-development-environment)
2. [Checking out source code](#checking-out-source-code)
3. [Building from source code](#building-from-source-code)
4. [Starting Fluvio cluster for development](#starting-fluvio-cluster-for-development)
5. [Running tests](#running-tests)
6. [Troubleshooting](#troubleshooting)

---

Thank you for joining Fluvio community. The goal of this document is to provide everything you need to get started with developing Fluvio.

Examples should work with the following platforms:

- MacOS X
- Linux  

Other platforms such as Windows can be made to work, but we haven't tried them yet.

To test and run services, you need to get access to development Kubernetes cluster.
Our guide uses Minikube as examples because it is easy to it get it started,
but you can use another Kubernetes cluster as well.
Please see [Kubernetes](https://kubernetes.io) for setting up a development cluster.

Please read [doc](www.fluvio.io) for technical arch and operation guide.

---

## Setting up Development Environment

The following are the tools that are used by development and automated testing

### Rust toolchain
* rustup
* cargo-fmt
* cargo-clippy

#### Set up Rust
Please follow [setup](https://www.rust-lang.org/tools/install) instructions to install Rust and Cargo.

### Buildtime dependencies
* make
* zig
* lld (v11)
* git

### Runtime dependencies

Kubernetes is required for running Fluvio.

* For development, please use one of the following supported kubernetes distros
    * [minikube](https://minikube.sigs.k8s.io/docs/start/)
    * [kind](https://kind.sigs.k8s.io)
    * [k3d](https://k3d.io)
* kubectl
* helm

#### Setting up Kubernetes Cluster

Fluvio supports the following Kubernetes cluster types for development:

* [minikube](https://minikube.sigs.k8s.io/docs/start/)
* [kind](https://kind.sigs.k8s.io)
* [k3d](https://k3d.io)

For these cluster types, fluvio will build a docker image and automatically imports it with `make build_k8_image`. 

For other cluster types, please file an issue.

Fluvio will run on any Kubernetes Cluster for non-development deployments.

#### Install Helm

Helm is used for installing Fluvio on Kubernetes.

Please follow [helm setup](https://helm.sh/docs/intro/quickstart/) to install helm

### Testing dependencies 
* curl
* jq
* bats

#### Install Bats-core

Bats-core is used for our CLI-based testing

Please follow the [bats-core](https://bats-core.readthedocs.io/en/stable/installation.html) installation guide.

## Checking out source code

You can clone the source code with the following command:
```
$ git clone https://github.com/infinyon/fluvio.git
```

## Building from source code

You can build from the source code using `make`.

Make targets

* `build-cli`: build only cli `fluvio`
* `build-cli-minimal`: build cli without Kubernetes admin
* `build-cluster`:  build platform component `fluvio-run` (SC and SPU)
* `build_k8_image`: build the kubernetes image and load it into your kubernetes distro's image registry

### Build Pre-requisites

Zig and LLD(version 11) is required to build the image.

For mac:

```
brew install zig
brew install llvm@11
export FLUVIO_BUILD_LLD=/opt/homebrew/Cellar/llvm\@11/11.1.0_3/bin/lld
```

For ubuntu LTS 20.04 or greater:

```
sudo snap install zig --beta --classic
sudo apt-get install -y lld-11
export FLUVIO_BUILD_LLD=lld-11
```

### Problem installing lld-11

If you have problem installing `lld-11`, please see: https://apt.llvm.org.

For ubuntu LTS 18.04:

```
wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key|sudo apt-key add -
sudo apt-get install clang-11 lldb-11 lld-11
```

## Building the Fluvio docker image

In order to deploy to Kubernetes, the Docker image version must be built and loaded into cluster.

Run following command to build the image and load into your kubernetes cluster's image registry

```
$ make build_k8_image
```

## Starting Fluvio cluster for development

### Optional: Download a published version of Fluvio

Instead of building Fluvio, you would prefer to just download it and get to work,
you can use our one-line installation script. You can use it to install the latest
release or prerelease, or to install a specific version:

```
$ curl -fsS https://packages.fluvio.io/v1/install.sh | bash                 # Install latest release
$ curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=latest bash  # Install latest pre-release
$ curl -fsS https://packages.fluvio.io/v1/install.sh | VERSION=x.y.z bash   # Install specific version
```


### Working with both Release and develop version of Flvuio

The next step is very important, as it will help you to prevent subtle development
bugs. Fluvio is built in two separate pieces, `fluvio` (the CLI), and `fluvio-run`
(the server). When testing changes to these components, you need to make sure to
rebuild _both_ components before running. In other Rust projects, it is typical to
just use `cargo run`:

```
$ cargo run -- my CLI args here
```

However, this will only rebuild `fluvio`, it will not also rebuild `fluvio-run`,
which may make you think that the code changes you made did not have any effect.
In order to automate the rebuilding of both of these components, we STRONGLY
RECOMMEND adding the following alias to your `~/.bashrc` or `~/.zshrc` file:

```
alias flvd='cargo build --manifest-path="/Users/nick/infinyon/fluvio/Cargo.toml" --bin fluvio-run && \
    cargo run --manifest-path="/Users/nick/infinyon/fluvio/Cargo.toml" --bin fluvio --'
```

Make sure to replace `/Users/nick/infinyon/fluvio` with the path where you cloned `fluvio`
on your own system. Then, the `flvd` command (short for "fluvio develop") will recompile
both `fluvio-run` and `fluvio`, then execute `fluvio` and pass the arguments to it.


### Kubernetes as a requirement

Kubernetes is currently a requirement for running Fluvio. We use Kubernetes to manage Fluvio's metadata. Running in "local" mode still requires kubernetes, however, Fluvio's processes run locally instead of within Kubernetes pods.


* Default mode: [Kubernetes-based Fluvio cluster](#kubernetes-based-fluvio-cluster)
* "local" mode: [OS-process based Fluvio cluster](#os-process-based-fluvio-cluster)

### Kubernetes-based Fluvio cluster

If you don't have an existing Kubernetes cluster, you can use following scripts to prepare your Kubernetes cluster for running Fluvio:

For minikube:
```
$ ./k8-util/cluster/reset-minikube.sh
```

For k3d: 
```
$ ./k8-util/cluster/reset-k3d.sh
```

For kind:
```
$ ./k8-util/cluster/reset-kind.sh
```


#### Setting Kubernetes up for running Fluvio in development

```
# This will build the Fluvio cli and then create a docker image
$ make build-cli build-cluster build_k8_image

# This will start a Fluvio cluster in Kubernetes using the cli and image from previous step
$ flvd cluster start --develop
```


In this mode, we run Fluvio components as Kubernetes objects.

#### Cleanup

Make sure you uninstall previous clusters for local and k8:

```
$ flvd cluster delete --local
$ flvd cluster delete
```

### Install Fluvio components as Kubernetes

Run command below now to run install with image just built

```
$ flvd cluster start --develop
```

Topic creation, product and consumer can now be tested as with `local` cluster.

You can remove fluvio cluster by

```
$ flvd cluster delete
```

Note that when you uninstall cluster, CLI will remove all related objects such as

- Topics
- Partitions
- Tls Secrets
- Storage

### OS-process based Fluvio cluster

In this mode, we run SC and SPU as the local process.

**Note: Running Kubernetes is still required**


We highly recommend using the `flvd cluster start --local --develop` command for most development.

However, in the following cases, we run `sc` and `spu` individually, allowing individual testing.

### Filtering Log

Please see [filering tracing log](https://tracing.rs/tracing_subscriber/filter/struct.envfilter).

### Starting SC

The Streaming Controller (SC) is the controller for a Fluvio cluster.
You only start a single SC for a single Fluvio cluster.

To run the SC, you'll need to build and run the `fluvio-run` executable:

```
$ RUST_LOG=fluvio=debug cargo run --bin fluvio-run -- sc
```

### Starting SPU

After SC is started, you can start adding unmanaged (custom) SPUs.

For each SPU, first register the SPU. For example, the following registers a SPU with ID 5001 with public and private ports. 
Normally, you only need to register a SPU once.

```
$ flvd cluster spu register --id 5001 --public-server 0.0.0.0:9010 --private-server  0.0.0.0:9011
```

Then you can start SPU 5001

```
$ cargo run --bin fluvio-run -- spu -i 5001 -p 0.0.0.0:9010 -v 0.0.0.0:9011 > /tmp/spu_5001.log
```

The logs can be found in `/tmp/spu_5001.log`.

Now, you should see SPU being active:

```
$ flvd cluster spu list
 ID    NAME             STATUS  TYPE      RACK  PUBLIC        PRIVATE 
 5001  custom-spu-5001  Online  "custom"   -    0.0.0.0:9010  0.0.0.0:9011 
```

Can create new topic
```
$ flvd topic create topic
topic "topic" created
```

Produce and consume works:
```
$ flvd produce topic
hello world
Ok!

$ flvd consume topic -B -d
hello world
```

You can launch additional SPU as needed, just ensure that ports doesn't conflict with each other.
For example, to add 2nd:

```
$ flvd cluster spu register --id 5001 --public-server 0.0.0.0:9020 --private-server  0.0.0.0:9021
$ cargo run --bin fluvio-run -- spu -i 5001 -p 0.0.0.0:9020 -v 0.0.0.0:9021
```

### Running SC in locally

First install fluvio k8 cluster as normally.

Then delete deployment:

```
kubectl delete deployment fluvio-sc
```

Then, can run sc directly

```
cargo run --bin fluvio-sc-k8
```
## Running tests

We have 3 types of tests:
- Tests run w/ `cargo`
    - This includes unit tests and doc tests
- Tests run with `fluvio-test`
    - These are integration tests executed with our `fluvio-test` test harness
    - Build with `make build-test`
- Tests run with `bats`
    - These are CLI tests written and executed with `bats-core`
    - Run with `make cli-smoke`

### Running local smoke test

Perform smoke test using local cluster mode:

```
make smoke-test-local
```

This results in message such as:
```
Creating the topic: test
topic "test" created
found topic: test offset: 0
starting fetch stream for: test base offset: 0, expected new records: 1000
<<consume test done for: test >>>>
consume message validated!, records: 1000
deleting cluster
```

### Running Kubernetes smoke test

Perform smoke test as Kubernetes objects:
```
make smoke-test-k8
```

### Running CLI smoke test

Perform CLI smoke test against your running cluster (Kubernetes or local)

```
make cli-smoke
```

## Troubleshooting

This guide helps users to solve issues they might face during the setup process. 

### Connection issues

If you face connection issues while creating minikube image

Re-build i.e. delete and restart minikube cluster

```
sh k8-util/minikube/reset-minikube.sh
```


### Fluvio sys chart issues

If you face issues while installing sys chart

```
$ flvd cluster start --sys
"fluvio" has been added to your repositories
Hang tight while we grab the latest from your chart repositories...
...Successfully got an update from the "fluvio" chart repository
Update Complete. ⎈ Happy Helming!⎈ 
Exited with status code: 1
thread 'main' panicked at 'assertion failed: false', crates/fluvio-cli/src/cluster/util.rs:115:17
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
```

Rebuilding minikube cluster sometimes doesnt remove the storage class. Hence the sys chart installation throws an error. Make sure the storage class is deleted.

```
kubectl delete storageclass fluvio-spu
```

### Deleting partition

In certain cases, partition may not be deleted correctly.  In this case, you can manually force delete by:
```
kubectl patch partition  <partition_name> -p '{"metadata":{"finalizers":null}}' --type merge
```