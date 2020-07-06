---
title: Custom SPUs
toc: true
weight: 50
---

**Custom SPUs** allow Fluvio **Streaming Controller** (**SC**) to identify and manage **SPU** services that are provisioned out-of-band. The **Custom-SPU** informs the **SC** that an **SPU** service will attach to the deployment at some point in the future. The **Custom-SPU** is used in the replica assignment as soon as it is configured. Initially it is marked offline until the **SPU** service connects to the **SC**. 

~> Custom-SPUs communicate with other SPUs though the internal protocol port. Ensure **internal traffic** is also encrypted if Custom-SPUs and Managed-SPUs communicate over a public medium.


## Generate and Deploy a Custom-SPU binary

Fluvio publishes and maintains SPU images in Docker Container format, other binary formats must be compiled from source code. Docker images are published at:

* [fluvio-sc](https://hub.docker.com/r/infinyon/fluvio-sc)
* [fluvio-spu](https://hub.docker.com/r/infinyon/fluvio-spu)

While **fluvio-spu** container can be used as a **Custom-SPU**, it is more common to compile an SPU image from source code directly on target.

-> Fluvio [Developer Guide](https://github.com/infinyon/fluvio/blob/master/DEVELOPER.md) in github provides step-by-step instructions on how to compile SC and SPU binaries from source code.


### Custom-SPUs Deployed outside Kubernetes cluster

**Custom-SPUs** that are deployed outside of your Kubernetes cluster need access to the SC internal channel. Run the following script to expose SC internal port:

```bash
$ kubectl apply -f k8-util/sc-deployment/sc-internal-dev.yaml 
```

#### On Minikube

Ensure **SC** private port **flv-sc-internal** load balancer has been created:

```bash
$ kubectl get services
NAME               TYPE           CLUSTER-IP       EXTERNAL-IP      PORT(S)             AGE
flv-sc-internal    LoadBalancer   10.111.202.47    10.111.202.47    9004:30314/TCP      4h25m
flv-sc-public      LoadBalancer   10.98.178.109    10.98.178.109    9003:31974/TCP      4h25m
flv-spg-group3     ClusterIP      None             <none>           9005/TCP,9006/TCP   4h9m
flv-spu-group3-0   LoadBalancer   10.105.174.231   10.105.174.231   9005:31368/TCP      4h9m
flv-spu-group3-1   LoadBalancer   10.105.169.200   10.105.169.200   9005:30391/TCP      4h9m
flv-spu-group3-2   LoadBalancer   10.101.143.60    10.101.143.60    9005:30080/TCP      4h9m
kubernetes         ClusterIP      10.96.0.1        <none>           443/TCP             4h34m
```

Save **SC** private port in an alias

```bash
$ alias SC-PRIVATE="kubectl get svc flv-sc-internal -o jsonpath='{.status.loadBalancer.ingress[0].ip}'"
```

The next steps must be performed in the following sequence:

1. Register **custom-spu** with the **SC**
2. Run **spu-server** binary compiled above


## Custom-SPU CLI

Custom-SPU module defines the following CLI operations: 

```bash
fluvio custom-spu <SUBCOMMAND>

SUBCOMMANDS:
    register    Register custom SPU
    unregister  Unregister custom SPU
    list        List custom SPUs
```

## Register Custom-SPU

Register **Custom-SPU** operation informs the **SC** that a custom **SPU** with the specific id is authorized to join to a **Fluvio** deployment. 

```bash
fluvio custom-spu register [OPTIONS] --id <id> --private-server <host:port> --public-server <host:port>

OPTIONS:
    -i, --id <id>                       SPU id
    -n, --name <string>                 SPU name
    -r, --rack <string>                 Rack name
    -p, --public-server <host:port>     Public server::port
    -v, --private-server <host:port>    Private server::port
    -c, --sc <host:port>                Address of Streaming Controller
    -P, --profile <profile>             Profile name
```

The options are defined as follows:

* **&dash;&dash;id &lt;id&gt;**:
is the identifier of the SPU that is authorized to be managed by a Fluvio deployment. The Custom-SPU id is compared with the SPU service id every time the service connects to the SC. SPU services that do not have a matching Custom-SPU id are rejected. The id is mandatory and it must be unique to the Fluvio deployment.

* **&dash;&dash;name &lt;string&gt;**:
is the name of the Custom-SPU. The name is optional and it is automatically generated if left empty. The format for auto-generated Custom-SPU names is: _spu-[id]_.

* **&dash;&dash;rack &lt;string&gt;**:
is the rack label of the Custom-SPU. Rack names have an impact on the *replica-assignment* when new topics are provisioned. The rack is an optional field.

* **&dash;&dash;public-server &lt;host:port&gt;**:
is the public interface of the Custom-SPU services. The public server information is used by Produce/Consumer to connect with the leader of a topic/partition. The public server is a mandatory field.

* **&dash;&dash;private-server &lt;host:port&gt;**:
is the private interface of the Custom-SPU service. SPUs establish private connections to negotiate leader election and replicate data from leaders to followers. The private server is a mandatory field.

* **&dash;&dash;sc &lt;host:port&gt;**:
is the public interface of the Streaming Controller. The SC is an optional field used in combination with [Cli Profiles](../#profiles) to compute a target service.

* **&dash;&dash;profile &lt;profile&gt;**:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service](..#target-service) section.


### Register Custom-SPU Example

Register **Custom-SPU** with the **SC**:

```bash
$ fluvio custom-spu register --id 200 --public-server `SC`:9005 --private-server `SC`:9006 --sc `SC`:9003
custom-spu 'custom-spu-200' registered successfully
```

Run **spu_server** :

```bash
$ spu-server --id 200 --sc-controller `SC-PRIVATE`:9004
starting custom-spu services (id:200)
```

Note that the SPU server must connect to the private interface and port number of the SC Controller.

Ensure **Custom-SPU** with id 200 has successfully joined the deployment and it is online.

```bash
$ fluvio spu list --sc `SC`:9003
ID   NAME            STATUS  TYPE     RACK  PUBLIC               PRIVATE 
200  custom-spu-200  online  custom    -    10.98.178.109:9005   10.98.178.109:9006 
```


## Unregister Custom-SPU

Unregister **Custom-SPU** operation informs the **SC** that the **SPU** is no longer authorized to participate in this **Fluvio** deployment. The **SC** rejects all new connections from the **SPU** service associated with this **Custom-SPU**.

```bash
$ fluvio custom-spu unregister [OPTIONS] --id <id>

OPTIONS:
    -i, --id <id>              SPU id
    -n, --name <string>        SPU name
    -c, --sc <host:port>       Address of Streaming Controller
    -P, --profile <profile>    Profile name
```

The options are defined as follows:

* **&dash;&dash;id &lt;id&gt;**:
is the identifier of the Custom-SPU to be detached. Id is a mandatory and mutually exclusive with &dash;&dash;name.

* **&dash;&dash;name &lt;string&gt;**:
is the name of the Custom-SPU to be detached. Name is a optional and mutually exclusive &dash;&dash;id.

* **&dash;&dash;sc &lt;host:port&gt;**:
See [Register Custom-SPU](#register-custom-spu)

* **&dash;&dash;profile &lt;profile&gt;**:
See [Register Custom-SPU](#register-custom-spu)

### Unregister Custom-SPU Example

Unregister **Custom-SPU**: 

```bash
$ fluvio custom-spu unregister --id 200 --sc `SC`:9003
custom-spu '200' deleted unregistered
```


## List Custom-SPUs

List **Custom-SPUs** operation lists all custom SPUs in a **Fluvio** deployment. 

```bash
$ fluvio custom-spu list [OPTIONS]

OPTIONS:
    -c, --sc <host:port>       Address of Streaming Controller
    -P, --profile <profile>    Profile name
    -O, --output <type>        Output [possible values: table, yaml, json]
```

The options are defined as follows:

* **&dash;&dash;sc &lt;host:port&gt;**:
See [Register Custom-SPU](#register-custom-spu)

* **&dash;&dash;profile &lt;profile&gt;**:
See [CLI Profiles](../profiles)

* **&dash;&dash;output &lt;type&gt;**:
is the format to be used to display the Custom-SPUs. The output is an optional field and it defaults to **table** format. Alternative formats are: **yaml** and **json**.

### List Custom-SPUs Example

List **Custom-SPUs**: 

```bash
$ fluvio custom-spu list --sc `SC`:9003
ID   NAME            STATUS  TYPE    RACK  PUBLIC              PRIVATE 
200  custom-spu-200  online  custom   -    10.98.178.109:9005  10.98.178.109:9006 
```


#### Related Topics
-------------------
* [Produce CLI](../produce)
* [Consume CLI](../consume)
* [SPUs CLI](../spus)
* [SPU-Groups CLI](../spu-groups)
* [Topics CLI](../topics)
