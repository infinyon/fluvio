# Fluvio Developer Guide

Table of contents:
- [Fluvio Developer Guide](#fluvio-developer-guide)
  - [Setting up Development Environment](#setting-up-development-environment)
    - [Rust toolchain](#rust-toolchain)
    - [Build time dependencies](#build-time-dependencies)
    - [Kubernetes dependencies](#kubernetes-dependencies)
      - [Helm](#helm)
    - [Linker Pre-requisites](#linker-pre-requisites)
  - [Building and running Fluvio cluster from source code for local binaries](#building-and-running-fluvio-cluster-from-source-code-for-local-binaries)
    - [Building the CLI binary](#building-the-cli-binary)
    - [Building the Cluster binary](#building-the-cluster-binary)
    - [Release profile](#release-profile)
    - [Inlining Helm chart](#inlining-helm-chart)
    - [Fluvio binaries Alias](#fluvio-binaries-alias)
    - [Running Fluvio cluster using native binaries](#running-fluvio-cluster-using-native-binaries)
      - [Re-Starting SC and SPU separately](#re-starting-sc-and-spu-separately)
      - [Deleting Fluvio cluster](#deleting-fluvio-cluster)
      - [System Chart](#system-chart)
      - [Setting Log level](#setting-log-level)
    - [Deleting Fluvio cluster](#deleting-fluvio-cluster-1)
  - [Building and running Fluvio cluster from source code for running in Kubernete cluster](#building-and-running-fluvio-cluster-from-source-code-for-running-in-kubernete-cluster)
      - [Starting Fluvio cluster using dev docker image](#starting-fluvio-cluster-using-dev-docker-image)
  - [Running tests](#running-tests)
    - [Testing dependencies](#testing-dependencies)
      - [Installing Bats-core](#installing-bats-core)
      - [Building smart modules](#building-smart-modules)
    - [Running local smoke test](#running-local-smoke-test)
    - [Running Kubernetes smoke test](#running-kubernetes-smoke-test)
    - [Running CLI smoke test](#running-cli-smoke-test)
  - [Troubleshooting](#troubleshooting)
    - [Connection issues](#connection-issues)
    - [Deleting partition](#deleting-partition)
  - [Optional: Download a published version of Fluvio](#optional-download-a-published-version-of-fluvio)

---

Thank you for joining the Fluvio community.  The goal of this document is to provide everything you need to start developing Fluvio.

Examples should work with the following platforms:

- macOS
- Linux

Other platforms such as Windows can be made to work, but we haven't tried them yet.

To test and run services, you need to get access to the development Kubernetes cluster.
Our guide uses Minikube as an example because it is easy to get it started, but you can use another Kubernetes cluster as well.
Please see [Kubernetes](https://kubernetes.io) for setting up a development cluster.

Please read [doc](https://www.fluvio.io) for a technical arch and operation guide.

---

## Setting up Development Environment

The development environment requires the Rust toolchain and a few other build dependencies to build changes to the source code.

Fluvio allows you to run the cluster locally or in a Kubernetes cluster, when
running on a Kubernetes Cluster metadata is stored in Kubernetes, as well as
system components such as the SC and SPU(s).

As an alternative, you can run Fluvio locally without Kubernetes, metadata will
be stored in disk and processes will spawn as system processes.

By default, Fluvio will create a cluster in the local environment unless you
specify it to run on a Kubernetes cluster.

### Rust toolchain

Please follow [setup](https://www.rust-lang.org/tools/install) instructions to install Rust and Cargo.

### Build time dependencies

Most of these are required for building a docker image: it is also possible to not install these and just `cargo build` the binaries you need and test them with a local setup.

* make
* zig
* lld (v16)
* git

### Kubernetes dependencies

Kubernetes is required for Fluvio to store its metadata.

Please set up one of the following recommended kubernetes distros:

* [OrbStack](https://orbstack.dev)
* [Rancher desktop](https://rancherdesktop.io)
* [k3d](https://k3d.io)
* [kind](https://kind.sigs.k8s.io)

#### Helm

Helm is used for installing Fluvio on Kubernetes.

Please follow [helm setup](https://helm.sh/docs/intro/quickstart/) to install the helm.

### Linker Pre-requisites

Zig and LLVM is required.

**macOS**

> [!NOTE]
> You can skip LLVM if you are not building docker image.

```bash
$ brew install llvm@16
$ ./actions/zig-install.sh
$ export PATH="/opt/homebrew/opt/llvm@16/bin:$PATH"
```

**Ubuntu**:

See https://apt.llvm.org for installing LLVM. LLVM up to 16 is confirmed to work.

```bash
$ ./actions/zig-install.sh
```

## Building and running Fluvio cluster from source code for local binaries

It is recommended to use native binaries for development and testing but not for production.

> [!IMPORTANT]
> For production, please build docker image and run in the Kuberentes as pod.

You still need to have Kubernets cluster running.

### Building the CLI binary

CLI is required to install, manage and access the Fluvio cluster.

To build CLI, run:

```
$ make build-cli
```

To build minimum CLI without Kubernetes dependencies for target such as Raspberry Pi, run:

```
$ make build-cli-minimal
```

### Building the Cluster binary

Entire Fluvio cluster (including SC and SPU) is contained a single binary.

To build the binary, run:

```
$ make build-cluster
```

### Release profile

By default, the build will use Rust `develop` profile.  To use `release` profile, set `RELEASE` to Makefile.

For example, to generate optimized binaries, run, `make build-cluster RELEASE=true`.

`make clean` to completely remove all build binaries and artifacts.

### Inlining Helm chart

Fluvio uses helm chart to install and manage Kubernetes components.  They are inline into Fluvio CLI binary.  If there is any issue with helm chart, run `make -C k8-util/helm clean` to clean up helm artifacts.


### Fluvio binaries Alias

Binaries are located in `target` directory.  You can run them directly or you can use following handy aliases:

```bash
alias flvd='target/debug/fluvio'
alias flvdr='target/release/fluvio'
alias flvt='target/debug/flv-test'
```

We will use the alias going forward.

### Running Fluvio cluster using native binaries

Use following commands to start Fluvio cluster using native binaries.

```bash
$ flvd cluster start --local --develop

📝 Running pre-flight checks
    ✅ Supported helm version 3.10.0+gce66412 is installed
    ✅ Kubectl active cluster rancher-desktop at: https://127.0.0.1:6443 found
    ✅ Supported Kubernetes server 1.22.7+k3s1 found
    ✅ Local Fluvio is not installed
    ✅ Fixed: Fluvio Sys chart 0.11.9 is installed
🎉 All checks passed!
✅ Local Cluster initialized
✅ SC Launched
👤 Profile set
🤖 Starting SPU: (1/1) /
✅ 1 SPU launched
🎯 Successfully installed Local Fluvio cluster
```

Then you can create topic, produce and consume messages.

```bash
$ flvd topic create hello
```

```bash
topic "hello" created
```

```bash
$ echo "hello world" | flvd produce hello
```

```bash
$ flvd consume hello -B
```

```bash
Consuming records from the beginning of topic 'hello'
hello world
⠒
```

> [!TIP]
> Press <kbd>Ctrl</kbd> + <kbd>C</kbd> to stop consuming messages.

You can see SC and SPU running:

```bash
$ ps -ef | grep fluvio
```

```bash
  501 61948     1   0  4:51PM ttys000    0:00.01 /tmp/fluvio/target/debug/fluvio run sc --local
  501 61949 61948   0  4:51PM ttys000    0:00.24 /tmp/fluvio/target/debug/fluvio-run sc --local
  501 61955     1   0  4:51PM ttys000    0:00.03 /tmp/fluvio/target/debug/fluvio run spu -i 5001 -p 0.0.0.0:9010 -v 0.0.0.0:9011 --log-base-dir /Users/myuser/.fluvio/data
  501 61956 61955   0  4:51PM ttys000    0:00.27 /tmpfluvio/target/debug/fluvio-run spu -i 5001 -p 0.0.0.0:9010 -v 0.0.0.0:9011 --log-base-dir /Users/myuser/.fluvio/data
  501 62035   989   0  4:52PM ttys000    0:00.00 grep fluvio
```


Since we still leverages Kubernetes CRDs, sys chart is still installed.

```bash
$ helm list
```

```bash
NAME            NAMESPACE       REVISION        UPDATED                                 STATUS          CHART                   APP VERSION
fluvio-sys      default         1               2022-10-06 18:46:23.359066 -0700 PDT    deployed        fluvio-sys-0.9.10       0.11.9
```


#### Re-Starting SC and SPU separately

During development, it is necessary to restart SC and SPU separately.

In order do so, you can kill SC or SPU and starting them individually.

You can use following commands for SC

```bash
$ kill -9 <process id of fluvio-run sc>
```

```bash
$ flvd run sc --local
```

```bash
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
Starting SC, platform: 0.11.9
Streaming Controller started successfully
```

You can then kill by <kbd>Ctrl</kbd> + <kbd>C</kbd>

> [!NOTE]
> This will not kill SPU. Once new SC is up, SPU will reconnect to it.

For SPU, you can use following template.

> [!IMPORTANT]
> `--log-base` should be same as the previously.

```bash
$ kill -9 <process id of fluvio-run spu>
```

```bash
flvd run spu -i 5001 -p 0.0.0.0:9010 -v 0.0.0.0:9011 --log-base-dir ~/.fluvio/data
```

```bash
starting spu server (id:5001)
SPU Version: 0.0.0 started successfully
```

You can launch additional SPU as needed; just ensure that ports don't conflict with each other.

For example, to add 2nd:

First register the new SPU:

```bash
$ flvd cluster spu register --id 5002 --public-server 0.0.0.0:9020 --private-server  0.0.0.0:9021
```

And then start the SPU:

```bash
$ flvd run spu -i 5002 -p 0.0.0.0:9020 -v 0.0.0.0:9021
```

#### Deleting Fluvio cluster

If Fluvio cluster is no longer needed, you can delete it using following command:

```bash
$ flvd cluster delete
```

This will print the following prompt:

```bash
✔ WARNING: You are about to delete local/127.0.0.1:9003. This operation is irreversible and the data stored in your cluster will be permanently lost.
Please type the cluster name to confirm: local <enter> (to confirm) / or CTRL-C (to cancel)
```

Write the cluster name `local` and press <kbd>Enter</kbd> to confirm deletion.

```bash
Deleting local/127.0.0.1:9003
Removed SPU monitoring socket
Uninstalled fluvio local components
```

#### System Chart

There are two helm charts that are installed by Fluvio CLI.

- `fluvio-sys` chart is installed when using native binaries.
- `fluvio-app` chart is installed when running Fluvio with docker image.

```bash
$ helm list
```

```bash
NAME            NAMESPACE       REVISION        UPDATED                                 STATUS          CHART                   APP VERSION
fluvio-sys      default         1               2022-10-06 19:18:37.416564 -0700 PDT    deployed        fluvio-sys-0.9.10       0.11.9
```

You can install system chart only using following command.  This assume system chart is not installed.

```bash
$ flvd cluster start --sys-only
```

```
installing sys chart, upgrade: false
```

#### Setting Log level

You can set various log levels [filering tracing log](https://tracing.rs/tracing_subscriber/filter/struct.envfilter).

For example, to start cluster using log level `info` using cluster start

```bash
$ flvd cluster start --local --develop --rust-log fluvio=info
```

For individual binaries, you can use `RUST_LOG` env variable:

```bash
$ RUST_LOG=fluvio=info flvd run sc --local
```

### Deleting Fluvio cluster

To remove all fluvio related objects in the Kubernetes cluster, you can use the following command:

```bash
$ flvd cluster delete
```

Note that when you uninstall the cluster, CLI will remove all related objects such as

- Topics
- Partitions
- Tls Secrets
- Storage
- etc


## Building and running Fluvio cluster from source code for running in Kubernete cluster

The docker image requires first installing a cross compilation toolchain, along with other build dependencies mentioned such as lld.

```bash
# x86_64 (most computers):
$ rustup target add x86_64-unknown-linux-musl
# M1 Macs:
$ rustup target add aarch64-unknown-linux-musl
```

This will build the Fluvio cli and then create a docker image and import it into your local k8s cluster:

```bash
$ make build-cli build_k8_image
```

If you are not running recommended version of k8s, image may not be imported into Kubernetes cluster.


#### Starting Fluvio cluster using dev docker image

This will run fluvio components as Kubernetes pods.


```bash
$ flvd cluster start --k8 --develop
```

```bash
using development git hash: c540c3a6ca488261edd20cdfdb95fdf50a050483

📝 Running pre-flight checks
    ✅ Kubectl active cluster rancher-desktop at: https://127.0.0.1:6443 found
    ✅ Supported helm version 3.10.0+gce66412 is installed
    ✅ Supported Kubernetes server 1.22.7+k3s1 found
    ✅ Fixed: Fluvio Sys chart 0.11.9 is installed
    ✅ Previous fluvio installation not found
🎉 All checks passed!
✅ Installed Fluvio app chart: 0.11.9
✅ Connected to SC: 192.168.50.106:30003
👤 Profile set
✅ SPU group main launched with 1 replicas
🎯 Successfully installed Fluvio

```

Then you can create topic, produce and consume messages as described above.

You should see two helm chart installed. There is additional chart `fluvio` that is used for installing fluvio components.

```bash
$ helm list
```

```bash
NAME            NAMESPACE       REVISION        UPDATED                                 STATUS          CHART                   APP VERSION
fluvio          default         1               2022-10-06 19:42:07.051782 -0700 PDT    deployed        fluvio-app-0.9.2        0.11.9
fluvio-sys      default         1               2022-10-06 19:42:06.668329 -0700 PDT    deployed        fluvio-sys-0.9.10       0.11.9
```

You should have two pods running:

```bash
$ kubectl get pods
```

```bash
NAME                        READY   STATUS    RESTARTS   AGE
fluvio-sc-fc976685d-qbxg2   1/1     Running   0          4m17s
fluvio-spg-main-0           1/1     Running   0          4m15s
```

And services for SC and SPG (SPU group) are running:

```bash
$ kubectl get service
```

```bash
NAME                 TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)             AGE
kubernetes           ClusterIP   10.43.0.1      <none>        443/TCP             112d
fluvio-sc-internal   ClusterIP   10.43.110.7    <none>        9004/TCP            5m8s
fluvio-sc-public     NodePort    10.43.31.194   <none>        9003:30003/TCP      5m8s
fluvio-spg-main      ClusterIP   None           <none>        9005/TCP,9006/TCP   5m6s
fluvio-spu-main-0    NodePort    10.43.88.71    <none>        9005:30004/TCP      5m6s
```

Fluvio uses `NodePort` to expose SC and SPU to the outside world.

And use PVC to store data:

```bash
$ kubectl get pvc
```

```bash
NAME                     STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
data-fluvio-spg-main-0   Bound    pvc-dff4c156-5718-4b41-a825-cee7d07fd997   10Gi       RWO            local-path     6m31s
```

Fluvio uses the default storage class used in the current Kubernetes but can be overridden using helm config.

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

### Testing dependencies

#### Installing Bats-core

Bats-core is used for our CLI-based testing.

Please follow the [bats-core](https://bats-core.readthedocs.io/en/stable/installation.html) installation guide.

#### Building smart modules

```bash
$ make build_smartmodules
```

### Running local smoke test

This requires a running cluster.

Perform smoke test using local cluster mode:

```bash
$ make smoke-test-local
```

This results in message such as:

```bash
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

```bash
$ make smoke-test-k8
```

### Running CLI smoke test

Perform CLI smoke test against your running cluster (Kubernetes or local)

```bash
$ make cli-fluvio-smoke
```

## Troubleshooting

This guide helps users to solve issues they might face during the setup process.

### Connection issues

If you face connection issues while creating minikube image

Re-build i.e. delete and restart minikube cluster

```bash
$ sh k8-util/minikube/reset-minikube.sh
```


### Deleting partition

In certain cases, partition may not be deleted correctly.  In this case, you can manually force delete by:

```bash
$ kubectl patch partition  <partition_name> -p '{"metadata":{"finalizers":null}}' --type merge
```

## Optional: Download a published version of Fluvio

Instead of building Fluvio, you may want to prefer just to download it and get to work.  You can use our one-line installation script.  You can use it to install the latest release or prerelease, or install a specific version:

**Install Stable Release**

```bash
$ curl -fsS https://hub.infinyon.cloud/install/install.sh | bash
```

**Install Latest Release (as of `master` branch)**

```bash
$ curl -fsS https://hub.infinyon.cloud/install/install.sh | VERSION=latest bash
```

**Install Specific Version**

```bash
$ curl -fsS https://hub.infinyon.cloud/install/install.sh | VERSION=x.y.z bash
```
