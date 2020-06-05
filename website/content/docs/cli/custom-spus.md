---
title: Custom SPUs
weight: 50
---

__Custom SPUs__ allow Fluvio __Streaming Controller__ (__SC__) to identify and manage __SPU__ services that are provisioned out-of-band. The __Custom-SPU__ informs the __SC__ that an __SPU__ service will attach to the deployment at some point in the future. The __Custom-SPU__ is used in the replica assignment as soon as it is configured. Initially it is marked offline until the __SPU__ service connects to the __SC__. 

{{< caution >}}
Custom-SPUs communicates with other SPUs though the private port. It is not recommended to mix Custom-SPUs with Managed-SPUs unless private port is visible to all services.
{{< /caution >}}

## Generate and Deploy a Custom-SPU binary

Fluvio publishes and maintains SPU images in Docker Container format, other binary formats must be compiled from source code. Docker images are published at:

* [fluvio-sc](https://hub.docker.com/r/infinyon/fluvio-sc)
* [fluvio-spu](https://hub.docker.com/r/infinyon/fluvio-spu)

While __fluvio-spu__ container can be used as a __Custom-SPU__, it is more common to compile an SPU image from source code directly on target.

{{< idea >}}
Fluvio [Developer Guide](https://github.com/infinyon/fluvio/blob/master/DEVELOPER.md) provides step-by-step instructions to compile an SPU image from source code.
{{< /idea >}}


### Custom-SPUs Deployed outside Kubernetes cluster

__Custom-SPUs__ that are deployed outside of your Kubernetes cluster need access to the SC internal channel. Run the following script to expose SC internal port:

{{< fluvio >}}
$ kubectl apply -f k8-util/sc-deployment/sc-internal-dev.yaml 
{{< /fluvio >}}

#### On Minikube

Ensure __SC__ private port __flv-sc-internal__ load balancer has been created:

{{< fluvio >}}
$ kubectl get services
NAME               TYPE           CLUSTER-IP       EXTERNAL-IP      PORT(S)             AGE
flv-sc-internal    LoadBalancer   10.111.202.47    10.111.202.47    9004:30314/TCP      4h25m
flv-sc-public      LoadBalancer   10.98.178.109    10.98.178.109    9003:31974/TCP      4h25m
flv-spg-group3     ClusterIP      None             <none>           9005/TCP,9006/TCP   4h9m
flv-spu-group3-0   LoadBalancer   10.105.174.231   10.105.174.231   9005:31368/TCP      4h9m
flv-spu-group3-1   LoadBalancer   10.105.169.200   10.105.169.200   9005:30391/TCP      4h9m
flv-spu-group3-2   LoadBalancer   10.101.143.60    10.101.143.60    9005:30080/TCP      4h9m
kubernetes         ClusterIP      10.96.0.1        <none>           443/TCP             4h34m
{{< /fluvio >}}

Save __SC__ private port in an alias

{{< fluvio >}}
$ alias SC-PRIVATE="kubectl get svc flv-sc-internal -o jsonpath='{.status.loadBalancer.ingress[0].ip}'"
{{< /fluvio >}}

The next steps must be performed in the following sequence:

1. Register __custom-spu__ with the __SC__
2. Run __spu-server__ binary compiled above


## Custom-SPU CLI

Custom-SPU module defines the following CLI operations: 

{{< fluvio >}}
fluvio custom-spu <SUBCOMMAND>

SUBCOMMANDS:
    register    Register custom SPU
    unregister  Unregister custom SPU
    list        List custom SPUs
{{< /fluvio >}}

## Register Custom-SPU

Register __Custom-SPU__ operation informs the __SC__ that a custom __SPU__ with the specific id is authorized to join to a __Fluvio__ deployment. 

{{< fluvio >}}
fluvio custom-spu register [OPTIONS] --id <id> --private-server <host:port> --public-server <host:port>

OPTIONS:
    -i, --id <id>                       SPU id
    -n, --name <string>                 SPU name
    -r, --rack <string>                 Rack name
    -p, --public-server <host:port>     Public server::port
    -v, --private-server <host:port>    Private server::port
    -c, --sc <host:port>                Address of Streaming Controller
    -P, --profile <profile>             Profile name
{{< /fluvio >}}

The options are defined as follows:

* <strong>{{< pre >}}--id &lt;id&gt;{{< /pre >}}</strong>:
is the identifier of the SPU that is authorized to be managed by a Fluvio deployment. The Custom-SPU id is compared with the SPU service id every time the service connects to the SC. SPU services that do not have a matching Custom-SPU id are rejected. The id is mandatory and it must be unique to the Fluvio deployment.

* <strong>{{< pre >}}--name &lt;string&gt;{{< /pre >}}</strong>:
is the name of the Custom-SPU. The name is optional and it is automatically generated if left empty. The format for auto-generated Custom-SPU names is: _spu-[id]_.

* <strong>{{< pre >}}--rack &lt;string&gt;{{< /pre >}}</strong>:
is the rack label of the Custom-SPU. Rack names have an impact on the *replica-assignment* when new topics are provisioned. The rack is an optional field.

* <strong>{{< pre >}}--public-server &lt;host:port&gt;{{< /pre >}}</strong>:
is the public interface of the Custom-SPU services. The public server information is used by Produce/Consumer to connect with the leader of a topic/partition. The public server is a mandatory field.

* <strong>{{< pre >}}--private-server &lt;host:port&gt;{{< /pre >}}</strong>:
is the private interface of the Custom-SPU service. SPUs establish private connections to negotiate leader election and replicate data from leaders to followers. The private server is a mandatory field.

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
is the public interface of the Streaming Controller. The SC is an optional field used in combination with [CLI Profiles]({{< relref "overview#profiles" >}}) to compute a target service.

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service]({{< relref "overview#target-service" >}}) section.


### Register Custom-SPU Example

Register __Custom-SPU__ with the __SC__:

{{< fluvio >}}
$ fluvio custom-spu register --id 200 --public-server `SC`:9005 --private-server `SC`:9006 --sc `SC`:9003
custom-spu 'custom-spu-200' registered successfully
{{< /fluvio >}}

Run __spu_server__ :

{{< fluvio >}}
$ spu-server --id 200 --sc-controller `SC-PRIVATE`:9004
starting custom-spu services (id:200)
{{< /fluvio >}}

Note that the SPU server must connect to the private interface and port number of the SC Controller.

Ensure __Custom-SPU__ with id 200 has successfully joined the deployment and it is online.

{{< fluvio >}}
$ fluvio spu list --sc `SC`:9003
ID   NAME            STATUS  TYPE     RACK  PUBLIC               PRIVATE 
200  custom-spu-200  online  custom    -    10.98.178.109:9005   10.98.178.109:9006 
{{< /fluvio >}}


## Unregister Custom-SPU

Unregister __Custom-SPU__ operation informs the __SC__ that the __SPU__ is no longer authorized to participate in this __Fluvio__ deployment. The __SC__ rejects all new connections from the __SPU__ service associated with this __Custom-SPU__.

{{< fluvio >}}
$ fluvio custom-spu unregister [OPTIONS] --id <id>

OPTIONS:
    -i, --id <id>              SPU id
    -n, --name <string>        SPU name
    -c, --sc <host:port>       Address of Streaming Controller
    -P, --profile <profile>    Profile name
{{< /fluvio >}}

The options are defined as follows:

* <strong>{{< pre >}}--id &lt;id&gt;{{< /pre >}}</strong>:
is the identifier of the Custom-SPU to be detached. Id is a mandatory and mutually exclusive with {{< pre >}}--name{{< /pre >}}.

* <strong>{{< pre >}}--name &lt;string&gt;{{< /pre >}}</strong>:
is the name of the Custom-SPU to be detached. Name is a optional and mutually exclusive {{< pre >}}--id{{< /pre >}}.

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
See [Register Custom-SPU](#register-custom-spu)

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
See [Register Custom-SPU](#register-custom-spu)

### Unregister Custom-SPU Example

Unregister __Custom-SPU__: 

{{< fluvio >}}
$ fluvio custom-spu unregister --id 200 --sc `SC`:9003
custom-spu '200' deleted unregistered
{{< /fluvio >}}


## List Custom-SPUs

List __Custom-SPUs__ operation lists all custom SPUs in a __Fluvio__ deployment. 

{{< fluvio >}}
$ fluvio custom-spu list [OPTIONS]

OPTIONS:
    -c, --sc <host:port>       Address of Streaming Controller
    -P, --profile <profile>    Profile name
    -O, --output <type>        Output [possible values: table, yaml, json]
{{< /fluvio >}}

The options are defined as follows:

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
See [Register Custom-SPU](#register-custom-spu)

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
See [Register Custom-SPU](#register-custom-spu)

* <strong>{{< pre >}}--output &lt;type&gt;{{< /pre >}}</strong>:
is the format to be used to display the Custom-SPUs. The output is an optional field and it defaults to __table__ format. Alternative formats are: __yaml__ and __json__.

### List Custom-SPUs Example

List __Custom-SPUs__: 

{{< fluvio >}}
$ fluvio custom-spu list --sc `SC`:9003
ID   NAME            STATUS  TYPE    RACK  PUBLIC              PRIVATE 
200  custom-spu-200  online  custom   -    10.98.178.109:9005  10.98.178.109:9006 
{{< /fluvio >}}


{{< links "Related Topics" >}}
* [Produce CLI]({{< relref "produce" >}})
* [Consume CLI]({{< relref "consume" >}})
* [SPUs CLI]({{< relref "spus" >}})
* [SPU-Groups CLI]({{< relref "spu-groups" >}})
* [Topics CLI]({{< relref "topics" >}})
{{< /links >}}