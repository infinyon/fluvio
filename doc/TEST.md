# Test Runner

Fluvio test runner is used to run test Fluvio platform.

## Assumption

* Fluvio system chart has been installed
* Kubernetes cluster has been setup

## Installation

Build Fluvio binaries
```
cargo build
```

Test runner can be located in: ```target/debug/flv-test```

## Cleaning up existing cluster

By default, ```flv-test``` will create new cluster.   Ensure you delete existing using fluvio:

```fluvio cluster uninstall``` for k8 or
```fluvio cluster uninstall --local``` for local cluster

## Alias

Set following alias to reduce cmd:

```
alias flvt='target/debug/flv-test'
```

## Running tests

Test runner performs following:
* Create new cluster
* Set up test topic
* Produce sets of message
* Consume message and validate them

Following test configuration can be changed
* Number of SPU ```--spu```  (default: 1)
* Number of parallel producer ```---produce-count``` (default: 1)
* Number of records to send by each produce ```---produce-iteration```  (default: 1)
* Record size ```---record-size```  (default: 100)
* Topic Name ```---topic-name```  (defaults to ```topic1```)
* Replication ```--replication``` ( default to 1)
* TLS (defaults to none)

### Test using Local cluster

```
flvt --local-driver
```

This creates local cluster with SC and 1 SPU.  

### Test using Kubernetes cluster

```
flvt
```

This use release version of fluvio image.  To run local image compiled from current code:
```make minikube_image```

and

```
flvt --local
```
