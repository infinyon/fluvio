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

## Requirements

Either for local or Kubernetes development, you need the following software to
be installed in your system:

- [Rust & Cargo](https://www.rust-lang.org/tools/install)
- [make](https://www.gnu.org/software/make/)
- [git](https://git-scm.com/)

### Kubernetes Cluster Development

> [!IMPORTANT]
> If you plan to develop Fluvio without Kubernetes, you can skip this section.

To run Fluvio on Kubernetes, you will need to install a Kubernetes
distribution, we recommend using one of:

* [OrbStack](https://orbstack.dev)
* [Rancher desktop](https://rancherdesktop.io)
* [k3d](https://k3d.io)
* [kind](https://kind.sigs.k8s.io)

You will also need **Helm** in order to install Fluvio charts.

Please follow [helm setup](https://helm.sh/docs/intro/quickstart/) to
install Helm.

### Linker Pre-requisites

- [LLVM v16](https://llvm.org)
- [Zig](https://ziglang.org)

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

## Building

It is recommended to use native binaries for development and testing but not for production.

> [!IMPORTANT]
> For production, please build docker image and run in the Kuberentes as pod.

The following section will guide you through building different Fluvio
components.

### Fluvio CLI

Fluvio CLI is the main entry point to interact with Fluvio cluster.  It is required to install, manage and access the Fluvio cluster.

Theres two ways to build the CLI, one includes all the features supported
to run on a regular environment, including Kubernetes support, and the other
one is a minimal version suitable for constrained environments like
_Raspberry Pi_ where you would run without Kubernetes features.

Use the following command to build the CLI binary with all features:

```bash
$ make build-cli
```

To build minimum CLI without Kubernetes dependencies for target such as
_Raspberry Pi_, run:

```bash
$ make build-cli-minimal
```

### Fluvio Cluster

Entire Fluvio cluster (including SC and SPU) is contained a single binary,
use the following command to build the cluster binary:

```bash
$ make build-cluster
```


### Pre-Build Versions

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

## Development

### Running Fluvio Cluster Locally

Use following commands to start Fluvio cluster using native binaries, this will
start Fluvio cluster locally without Kubernetes.

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
Consuming records from 'hello' starting from the beginning of log
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

### Running Fluvio Cluster in Kubernetes

> [!IMPORTANT]
> Make sure your Kubernetes cluster is running and `kubectl` is configured to access the cluster.

To create a Fluvio cluster in Kubernetes, run the following command:

```bash
flvd cluster start --k8 --develop
```

> [!TIP]
> Note that the `--k8` flag is used to start Fluvio cluster in Kubernetes

Expect the following output:

```bash
📝 Running pre-flight checks
    ✅ Kubectl active cluster minikube at: https://127.0.0.1:32771 found
    ✅ Supported helm version 3.15.0+gc4e37b3 is installed
    ✅ Supported Kubernetes server 1.30.0 found
    ✅ Fixed: Fluvio Sys chart 0.11.9 is installed
    ✅ Previous fluvio installation not found
🎉 All checks passed!
✅ Installed Fluvio app chart: 0.11.9
 -
👤 Profile set
🖥️  Trying to connect to SC: localhost:30003 0 seconds elapsed \
✅ Connected to SC: localhost:30003
🖥️  Trying to connect to SC: localhost:30003 0 seconds elapsed \
🖥️ Waiting for SPUs to be ready and have ingress... (timeout: 300s) -
...
🖥️ 1/1 SPU confirmed, 3 seconds elapsed /
✅ SPU group main launched with 1 replicas
🖥️ 1/1 SPU confirmed, 3 seconds elapsed /
🎯 Successfully installed Fluvio!
```

Fluvio leverages Kubernetes CRDs to manage Fluvio components, sys chart is installed using Helm. You can list these installed charts using the following command:

```bash
$ helm list
```

You should see two helm chart installed. There is additional chart `fluvio` that is used for installing fluvio components.

```bash
NAME      	NAMESPACE	REVISION	UPDATED                             	STATUS  	CHART            	APP VERSION
fluvio    	default  	1       	2024-06-18 12:45:36.917241 -0400 -04	deployed	fluvio-app-0.9.3 	0.11.9
fluvio-sys	default  	1       	2024-06-18 12:45:36.499479 -0400 -04	deployed	fluvio-sys-0.9.18	0.11.9
```

Inspect running pods spawned by Fluvio:

```bash
$ kubectl get pods
```

You should have two pods running:

```bash
NAME                         READY   STATUS    RESTARTS   AGE
fluvio-sc-7f64bffbc6-b28zw   1/1     Running   0          7m9s
fluvio-spg-main-0            1/1     Running   0          7m3s
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

Inspect logs from each pod using the following command:

```bash
kubectl logs <POD NAME>
```

Consider the output above, you can inspect logs from SC pod using the following command:

```bash
kubectl logs fluvio-sc-7f64bffbc6-b28zw
```

Similar to how its performed for local cluster, you can create topics, produce and consume messages.

```bash
$ flvd topic create hello
```

```bash
topic "hello" created
```

Produce:

```bash
$ flvd produce hello
```

```bash
> hello world
```

Consume:

```bash
$ flvd consume -B hello
```

```bash
Consuming records from 'hello' starting from the beginning of log
hello world
```

## Post-Development

### Deleting Fluvio cluster


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

Note that when you uninstall the cluster, CLI will remove all related objects such as

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

## DevEx & Troubleshooting

### Release Profile

By default, the build will use Rust `debug` profile, this is suitable for
debugging and development. The final binary will be bigger and slower than
the `release` profile, but build times will be quicker.

To use `release` profile, set `RELEASE` to Makefile, this profile optimizes
the output binary for production environments but it takes more time to build.

For example, to generate optimized binaries, run:

```bash
$ make build-cluster RELEASE=true`
```

To clean up the build artifacts and build generated files, run:

```bash
$ make clean
```

### Inlining Helm Chart

Fluvio uses Helm Chart to install and manage Kubernetes components.
These are inlined into the Fluvio CLI binary. If there is any issue with Helm Chart, run the following command to clean up:

```bash
$ make -C k8-util/helm clean
```

### Aliasing Fluvio Development Binaries

Binaries are located in `target` directory. You can run them directly or you can use following handy aliases:

```bash
alias flvd='target/debug/fluvio'
alias flvdr='target/release/fluvio'
alias flvt='target/debug/flv-test'
```

> [!NOTE]
> We will use the alias going forward.

### Re-Starting SC and SPU separately

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

### Adding Additional SPU

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

### Running on macOS with Minikube (Kubernetes Setup)

If you are running on macOS with Minikube we recommend exposing the following
ports when spawning the Minikube cluster:

```bash
minikube start --ports 9003,9005,30003:30003,30004:30004
```

This exposes internal/external ports from Fluvio SC running on Minikube to the
host machine, as well as maps the ports used to reach the SPU from the
container to the host machine.

### System Chart (Kubernetes Setup)

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

```bash
installing sys chart, upgrade: false
```

### Setting Log level

You can set various log levels [filering tracing log](https://tracing.rs/tracing_subscriber/filter/struct.envfilter).

For example, to start cluster using log level `info` using cluster start

```bash
$ flvd cluster start --local --develop --rust-log fluvio=info
```

For individual binaries, you can use `RUST_LOG` env variable:

```bash
$ RUST_LOG=fluvio=info flvd run sc --local
```

### Building and Running Fluvio Cluster from source code for running in Kubernete cluster

The docker image requires first installing a cross compilation toolchain, along with other build dependencies mentioned such as `lld`.

**x86/64 (most computers)**

```bash
$ rustup target add x86_64-unknown-linux-musl
```

**Apple Silicon**

```bash
$ rustup target add aarch64-unknown-linux-musl
```

This will build the Fluvio cli and then create a docker image and import it into your local k8s cluster:

```bash
$ make build-cli build_k8_image
```

> [!IMPORTANT]
> If you are not running recommended version of k8s, image may not be imported
> into Kubernetes cluster.

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
