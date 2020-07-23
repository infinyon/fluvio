# Installation

## Install CLI

Fluvio comes with CLI which can be used install, manage topic, produce and consume messages.

* First download Fluvio CLI binaries [github release](https://github.com/infinyon/fluvio/releases) for your OS.
* Copy CLI binary to your bin path and make it executable.

To test CLI binary:
```
$ fluvio version

version is: 0.4.1
```

Fluvio can be installed in any Kubernetes cluster.  For running in your laptop, it is recommended to use minikube.  There is special instruction for minikube for or cluster which uses IP address.  After minikube is setup, please follow regular kubernetes installation sections

## Set up Minikube

Install [Minikube](https://www.fluvio.io/docs/getting-started/minikube/) and create new cluster

```
minikube start
```

To work around certificate issue with IP address, use following create kubectl context which create dummy DNS rather than IP address
```
fluvio cluster set-minikube-context
```

This will create new kubectl context ```mycube``` and switch to it.

finally, start minikube tunnel which let you create external load balancer:

```
sudo nohup  minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out &
```

If you want to terminate tunnel, you can do
```
sudo pkill minikube
rm /tmp/tunnel.*
```

## Install Helm binary

Fluvio use Helm chart for installation.

Please follow [Helm](https://helm.sh) doc to download and setup helm binary.  Once it is setup, you can use following command to test see if it is properly installed:

```
helm list
```


## Installing Fluvio system chart

Fluvio system chart will install cluster-wide components.  This is only needed to be done once per Kubernetes cluster.  

This is only step where cloud specific components are configured.  For example, minikube and eks have different driver for storage.

Default is minikube.

```
$ fluvio cluster install --sys
```

For eks,
```
$ fluvio cluster install -sys --cloud eks
```


## Installing Fluvio cluster

Each Fluvio cluster consist of at least 1 controller and multiple SPU. A Fluvio cluster can be installed per namespace.  

To install a cluster:
```
$ fluvio cluster install --namespace <namespace>
```

To install on default name space.
```
$ fluvio cluster install
```

After installation, spu should see them provisioned:
```
$ fluvio spu list
flvd spu list
 ID  NAME    STATUS  TYPE     RACK  PUBLIC              PRIVATE 
  0  main-0  online  managed   -    10.102.55.151:9005  flv-spg-main-0.flv-spg-main:9006 
```

## Quick Start

To perform a simple consume and producer test:

First. a create a simple topic name ```message```

```
$ fluvio topic create message
topic "message" created
```

Produce a text to message topic:
```
$ fluvio produce message
hello world
Ok!
```
Then we can consume or retrieve message.  In this case, we retrieve from beginning.  By default, messages are retrieval from last.  "-d" terminate immediately instead of waiting for next message.
```
$ flvd consume message -B -d
hello world
```

You can retrieve from known offset. 
```
flvd consume message -o 0
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
