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

Please read [doc](https://www.fluvio.io) for technical arch and operation guide.

---

## Setting up Development Environment


### Rust toolchain


Please follow [setup](https://www.rust-lang.org/tools/install) instructions to install Rust and Cargo.

### Buildtime dependencies
* make
* zig
* lld (v14)
* git

### Kubernetes dependencies

Kubernetes is required for running Fluvio.

Following Kubernetes distribution please use one of the following supported kubernetes distros to set up Kubernetes Cluster

* [Rancher desktop](https://rancherdesktop.io)
* [k3d](https://k3d.io)
* [minikube](https://minikube.sigs.k8s.io/docs/start/)
* [kind](https://kind.sigs.k8s.io)

#### Installing Helm

Helm is used for installing Fluvio on Kubernetes.

Please follow [helm setup](https://helm.sh/docs/intro/quickstart/) to install helm

### Testing dependencies

#### Installing Bats-core

Bats-core is used for our CLI-based testing

Please follow the [bats-core](https://bats-core.readthedocs.io/en/stable/installation.html) installation guide.

## Checking out source code

You can clone the source code with the following command:
```
$ git clone https://github.com/infinyon/fluvio.git
```

## Make targets

You can build from the source code using `make`.  The following targets are available:

* `build-cli`: build CLI binary.
* `build-cli-minimal`: build cli without Kubernetes admin.
* `build-cluster`:  build native platform binaries (SC and SPU) to run directly on your OS.
* `build_k8_image`: build the kubernetes image and load it into your kubernetes distro's image registry

### Build Pre-requisites

Zig and LLD(version 12 or higher) is required to build the image.

For mac:

```
./actions/zig-install.sh macos-11
export FLUVIO_BUILD_LLD=/opt/homebrew/Cellar/llvm@14/bin/lld
```

For ubuntu:

```
./actions/zig-install.sh ubuntu-latest
export FLUVIO_BUILD_LLD=lld-12
```

### Problem installing lld

If you have problem installing `lld-14`, please see: https://apt.llvm.org.

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

If you don't have an existing Kubernetes cluster, you can use following scripts to prepare your Kubernetes cluster for running Fluvio.
This is not required if you have an existing K8 cluster such as Rancher desktop.

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


#### Build Fluvio CLI and docker image from source code

```
# This will build the Fluvio cli and then create a docker image
$ make build-cli build_k8_image
```

#### Starting Fluvio cluster using dev docker image

This will run fluvio components as Kubernetes pods.

```
$ flvd cluster start --develop

using development git hash: a816d22830a0e5dc2f58cdd49765d91117c84a13

📝 Running pre-flight checks
    ✅ Kubectl active cluster rancher-desktop at: https://127.0.0.1:6443 found
    ✅ Supported helm version 3.7.1+g1d11fcb is installed
    ✅ Supported Kubernetes server 1.22.7+k3s1 found
    ✅ Fixed: Fluvio Sys chart 0.9.32 is installed
    ✅ Previous fluvio installation not found
🎉 All checks passed!
✅ Installed Fluvio app chart: 0.9.32
✅ Connected to SC: 192.168.50.106:30003
👤 Profile set
✅ SPU group main launched with 1 replicas
🎯 Successfully installed Fluvio!

```

Then you can create topic, produce and consume messages.

You should see two helm chart installed:
```
$> helm list
NAME            NAMESPACE       REVISION        UPDATED                                 STATUS          CHART                   APP VERSION
fluvio          default         1               2022-07-20 16:41:40.381758 -0700 PDT    deployed        fluvio-app-0.9.2        0.9.31     
fluvio-sys      default         1               2022-07-20 16:41:38.112869 -0700 PDT    deployed        fluvio-sys-0.9.9        0.9.31 
```

There is always `fluvio-sys` chart installed.  For kubernetes, `fluvio` chart is installed.
Helm charts are generated from CLI installer instead of from helm registry. 

You should two pods running:
```
$> kubectl get pods
NAME                        READY   STATUS    RESTARTS   AGE
fluvio-sc-fc976685d-qbxg2   1/1     Running   0          4m17s
fluvio-spg-main-0           1/1     Running   0          4m15s
```

And services for SC and SPG (SPU group) are running:
```
$> kubectl get service
NAME                 TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)             AGE
kubernetes           ClusterIP   10.43.0.1      <none>        443/TCP             112d
fluvio-sc-internal   ClusterIP   10.43.110.7    <none>        9004/TCP            5m8s
fluvio-sc-public     NodePort    10.43.31.194   <none>        9003:30003/TCP      5m8s
fluvio-spg-main      ClusterIP   None           <none>        9005/TCP,9006/TCP   5m6s
fluvio-spu-main-0    NodePort    10.43.88.71    <none>        9005:30004/TCP      5m6s
```

Fluvio uses `NodePort` to expose SC and SPU to outside world.

And storage provisioning:
```
$> kubectl get pvc
NAME                     STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
data-fluvio-spg-main-0   Bound    pvc-dff4c156-5718-4b41-a825-cee7d07fd997   10Gi       RWO            local-path     6m31s
```

Fluvio uses default storageclass used in the current Kubernetes but can be overriden using helm config. 

### Running Fluvio cluster using native binaries

In this mode, we run SC and SPU as the local process.  This makes it easier to run and troubleshoot Fluvio locally, but it is not recommended for production use.

**Note: Running Kubernetes is still required**

#### Building Fluvio CLI and native binaries

In the native mode, Fluvio binarie uses native OS call rather than Linux API.

```
$ make build-cli build-cluster
```

#### Starting the Fluvio cluster using native binaries

Make sure you remove existing Fluvio cluster first.

```
$ flvd cluster delete
Current channel: stable
Uninstalled fluvio kubernetes components
Uninstalled fluvio local components
Objects and secrets have been cleaned up
```

Then you can start Fluvio cluster using local mode which uses native binaries:

```
$ flvd cluster start --local --develop

📝 Running pre-flight checks
    ✅ Supported helm version 3.7.1+g1d11fcb is installed
    ✅ Kubectl active cluster rancher-desktop at: https://127.0.0.1:6443 found
    ✅ Supported Kubernetes server 1.22.7+k3s1 found
    ✅ Local Fluvio is not installed
    ✅ Fixed: Fluvio Sys chart 0.9.32 is installed
🎉 All checks passed!
✅ Local Cluster initialized
✅ SC Launched
👤 Profile set
✅ 1 SPU launched
🎯 Successfully installed Local Fluvio cluster
```

Then you can create topic, produce and consume messages.

You can see processes:
```
ps -ef | grep fluvio
  501 61948     1   0  4:51PM ttys000    0:00.01 /tmp/fluvio/target/debug/fluvio run sc --local
  501 61949 61948   0  4:51PM ttys000    0:00.24 /tmp/fluvio/target/debug/fluvio-run sc --local
  501 61955     1   0  4:51PM ttys000    0:00.03 /tmp/fluvio/target/debug/fluvio run spu -i 5001 -p 0.0.0.0:9010 -v 0.0.0.0:9011 --log-base-dir /Users/myuser/.fluvio/data
  501 61956 61955   0  4:51PM ttys000    0:00.27 /tmpfluvio/target/debug/fluvio-run spu -i 5001 -p 0.0.0.0:9010 -v 0.0.0.0:9011 --log-base-dir /Users/myuser/.fluvio/data
  501 62035   989   0  4:52PM ttys000    0:00.00 grep fluvio
```

There are 2 processes for each SC and SPU because there are wrapper processes for SC and SPU.

Since we still leverages Kubernetes CRDs, sys chart is still installed.
```
$> helm list
NAME            NAMESPACE       REVISION        UPDATED                                 STATUS          CHART                   APP VERSION
fluvio-sys      default         1               2022-07-20 16:51:25.098218 -0700 PDT    deployed        fluvio-sys-0.9.9        0.9.31 
```

We highly recommend using the `flvd cluster start --local --develop` command for most development.

However, in the following cases, we run `sc` and `spu` individually, allowing individual testing.


#### Starting SC and SPU separately

This is useful if you want to test SC or SPU independently.


Delete the cluster first:
```
$ flvd cluster delete
Current channel: stable
Uninstalled fluvio kubernetes components
Uninstalled fluvio local components
Objects and secrets have been cleaned up
```

Install sys-chart only:
```
$> flvd cluster start --sys-only
installing sys chart, upgrade: false
$> helm list
NAME            NAMESPACE       REVISION        UPDATED                                 STATUS          CHART                   APP VERSION
fluvio-sys      default         1               2022-07-20 18:56:27.130405 -0700 PDT    deployed        fluvio-sys-0.9.9        0.9.31  
```

##### Starting SC by itself

To run SC binary only:

```
cargo run --bin fluvio-run sc --local

$> cargo run --bin fluvio-run sc --local
    
    Finished dev [unoptimized + debuginfo] target(s) in 0.27s
     Running `target/debug/fluvio-run sc --local`
CLI Option: ScOpt {
    local: true,
    bind_public: None,
    bind_private: None,
    namespace: None,
    tls: TlsConfig {
        tls: false,
        server_cert: None,
        server_key: None,
        enable_client_cert: false,
        ca_cert: None,
        bind_non_tls_public: None,
    },
    x509_auth_scopes: None,
    auth_policy: None,
    white_list: [],
}
Starting SC, platform: 0.9.32
Streaming Controller started successfully

```

At this point, you can use control-c to stop the process or control-z to put in background.


##### Starting SPU

After SC is started, you can start adding unmanaged (custom) SPUs.

For each SPU, first register the SPU. For example, the following registers a SPU with ID 5001 with public and private ports.
Normally, you only need to register a SPU once.

```
$ flvd cluster spu register --id 5001 --public-server 0.0.0.0:9010 --private-server  0.0.0.0:9011
```

Then you can start SPU 5001

```
$ cargo run --bin fluvio-run -- spu -i 5001 -p 0.0.0.0:9010 -v 0.0.0.0:9011 --log-base-dir ~/.fluvio/data
```

Similar to SC, you can use control-c to stop the process or control-z to put in background.

You can see SPU status:
```
$ flvd cluster spu list
 ID    NAME             STATUS  TYPE      RACK  PUBLIC        PRIVATE
 5001  custom-spu-5001  Online  "custom"   -    0.0.0.0:9010  0.0.0.0:9011
```

You can launch additional SPU as needed, just ensure that ports doesn't conflict with each other.
For example, to add 2nd:

```
$ flvd cluster spu register --id 5002 --public-server 0.0.0.0:9020 --private-server  0.0.0.0:9021
$ cargo run --bin fluvio-run -- spu -i 5002 -p 0.0.0.0:9020 -v 0.0.0.0:9021
```


### Setting Log level

You can set variou log level [filering tracing log](https://tracing.rs/tracing_subscriber/filter/struct.envfilter).

For example, to start cluster using log level `info` using cluster start
```
flvd cluster start --local --develop --rust-log fluvio=info
```

For individual binaries, you can use RUST_LOG env variable:
```
RUST_LOG=fluvio=info cargo run --bin fluvio-run sc --local
```

### Deleting Fluvio cluster


To remove all fluvio related object in the Kubernetes cluster, you can use the following command:

```
$ flvd cluster delete
```

Note that when you uninstall cluster, CLI will remove all related objects such as

- Topics
- Partitions
- Tls Secrets
- Storage
- etc
  
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


### Deleting partition

In certain cases, partition may not be deleted correctly.  In this case, you can manually force delete by:
```
kubectl patch partition  <partition_name> -p '{"metadata":{"finalizers":null}}' --type merge
```
