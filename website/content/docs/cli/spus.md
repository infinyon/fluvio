---
title: SPUs
weight: 50
---

The __SPUs__, also known as __Streaming Processing Units__, are the service engines responsible for processing data streams. A data stream has one leader and one or more followers. The leader and the followers are evenly distributed across the __SPUs__.

Fluio supports two types of SPUs:

* __Managed SPUs__: defined in [SPU-Groups CLI]({{< relref "spu-groups" >}}).
* __Custom SPUs__: defined in [Custom-SPUs CLI]({{< relref "custom-spus" >}})

SPU module defines the CLI operations that are common both SPU types: 

{{< fluvio >}}
fluvio spu <SUBCOMMAND>

SUBCOMMANDS:
    list    List custom & managed SPUs
{{< /fluvio >}}


## List SPUs

List __SPUs__ operation lists all managed and custom SPUs in a __Fluvio__ deployment. 

{{< fluvio >}}
fluvio spu list [OPTIONS]

OPTIONS:
    -c, --sc <host:port>       Address of Streaming Controller
    -P, --profile <profile>    Profile name
    -O, --output <type>        Output [possible values: table, yaml, json]
{{< /fluvio >}}

The options are defined as follows:

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
is the public interface of the Streaming Controller. The SC is an optional field used in combination with [CLI Profiles]({{< relref "overview#profiles" >}}) to compute a target service.

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service]({{< relref "overview#target-service" >}}) section.

* <strong>{{< pre >}}--output &lt;type&gt;{{< /pre >}}</strong>:
is the format to be used to display the SPUs. The output is an optional field and it defaults to __table__ format. Alternative formats are: __yaml__ and __json__.

### List SPUs Example

All list commands support 3 format types: __table__, __yaml__ and __json__. The following examples show _List SPU_ command in all three format types:

#### TABLE format (default)

{{< fluvio >}}
$ fluvio spu list  --sc `SC`:9003
ID  NAME      STATUS  TYPE     RACK  PUBLIC               PRIVATE 
0  group3-0  online  managed   -    10.105.174.231:9005  flv-spg-group3-0.flv-spg-group3:9006 
1  group3-1  online  managed   -    10.105.169.200:9005  flv-spg-group3-1.flv-spg-group3:9006 
2  group3-2  online  managed   -    10.101.143.60:9005   flv-spg-group3-2.flv-spg-group3:9006 
{{< /fluvio >}}

#### YAML format

{{< cli >}}
$ fluvio spu list  --sc `SC`:9003 -O yaml
---
- name: group3-0
  spu:
    id: 0
    name: group3-0
    spu_type: Managed
    public_server:
      host: 10.105.174.231
      port: 9005
    private_server:
      host: flv-spg-group3-0.flv-spg-group3
      port: 9006
    status: Online
- name: group3-1
  spu:
    id: 1
    name: group3-1
    spu_type: Managed
    public_server:
      host: 10.105.169.200
      port: 9005
    private_server:
      host: flv-spg-group3-1.flv-spg-group3
      port: 9006
    status: Online
- name: group3-2
  spu:
    id: 2
    name: group3-2
    spu_type: Managed
    public_server:
      host: 10.101.143.60
      port: 9005
    private_server:
      host: flv-spg-group3-2.flv-spg-group3
      port: 9006
    status: Online
{{< /cli >}}


#### JSON format

{{< cli json >}}
$ fluvio spu list  --sc `SC`:9003 -O json
[
  {
    "name": "group3-0",
    "spu": {
      "id": 0,
      "name": "group3-0",
      "spu_type": "Managed",
      "public_server": {
        "host": "10.105.174.231",
        "port": 9005
      },
      "private_server": {
        "host": "flv-spg-group3-0.flv-spg-group3",
        "port": 9006
      },
      "status": "Online"
    }
  },
  {
    "name": "group3-1",
    "spu": {
      "id": 1,
      "name": "group3-1",
      "spu_type": "Managed",
      "public_server": {
        "host": "10.105.169.200",
        "port": 9005
      },
      "private_server": {
        "host": "flv-spg-group3-1.flv-spg-group3",
        "port": 9006
      },
      "status": "Online"
    }
  },
  {
    "name": "group3-2",
    "spu": {
      "id": 2,
      "name": "group3-2",
      "spu_type": "Managed",
      "public_server": {
        "host": "10.101.143.60",
        "port": 9005
      },
      "private_server": {
        "host": "flv-spg-group3-2.flv-spg-group3",
        "port": 9006
      },
      "status": "Online"
    }
  }
]
{{< /cli >}}


{{< links "Related Topics" >}}
* [Produce CLI]({{< relref "produce" >}})
* [Consume CLI]({{< relref "consume" >}})
* [Custom SPU CLI]({{< relref "custom-spus" >}})
* [SPU-Groups CLI]({{< relref "spu-groups" >}})
* [Topics CLI]({{< relref "topics" >}})
{{< /links >}}