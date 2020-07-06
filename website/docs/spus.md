---
title: SPUs
toc: true
weight: 50
---

The **SPUs**, also known as **Streaming Processing Units**, are the service engines responsible for processing data streams. A data stream has one leader and one or more followers. The leader and the followers are evenly distributed across the **SPUs**. For additional information on leader and follower replica distribution, see [Fluvio Architecture](/docs/architecture).

Fluvio supports two types of SPUs:

* **Managed SPUs**: defined in [SPU-Groups CLI](../spu-groups).
* **Custom SPUs**: defined in [Custom SPU CLI](../custom-spus)

SPU module defines the CLI operations that are common both SPU types: 

```bash
fluvio spu <SUBCOMMAND>

SUBCOMMANDS:
    list    List custom & managed SPUs
```


## List SPUs

List **SPUs** operation lists all managed and custom SPUs in a **Fluvio** deployment. 

```bash
fluvio spu list [OPTIONS]

OPTIONS:
    -c, --sc <host:port>       Address of Streaming Controller
    -P, --profile <profile>    Profile name
    -O, --output <type>        Output [possible values: table, yaml, json]
```

The options are defined as follows:

* **&dash;&dash;sc &lt;host:port&gt;**:
is the public interface of the Streaming Controller. The SC is an optional field used in combination with Cli Profiles](../#profiles) to compute a target service.

* **&dash;&dash;profile &lt;profile&gt;**:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service](..#target-service) section.

* **&dash;&dash;output &lt;type&gt;**:
is the format to be used to display the SPUs. The output is an optional field and it defaults to **table** format. Alternative formats are: **yaml** and **json**.

### List SPUs Example

All list commands support 3 format types: **table**, **yaml** and **json**. The following examples show _List SPU_ command in all three format types:

#### TABLE format (default)

```bash
$ fluvio spu list  --sc `SC`:9003
ID  NAME      STATUS  TYPE     RACK  PUBLIC               PRIVATE 
0  group3-0  online  managed   -    10.105.174.231:9005  flv-spg-group3-0.flv-spg-group3:9006 
1  group3-1  online  managed   -    10.105.169.200:9005  flv-spg-group3-1.flv-spg-group3:9006 
2  group3-2  online  managed   -    10.101.143.60:9005   flv-spg-group3-2.flv-spg-group3:9006 
```

#### YAML format

List SPUs in YAML format:

```bash
$ fluvio spu list  --sc `SC`:9003 -O yaml
```

produces:

```yaml
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
```


#### JSON format

List SPUs in JSON format:

```bash
$ fluvio spu list  --sc `SC`:9003 -O json
```

produces:

```json
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
```


#### Related Topics
-------------------
* [Produce CLI](../produce)
* [Consume CLI](../consume)
* [Custom SPU CLI](../custom-spus)
* [SPU-Groups CLI](../spu-groups)
* [Topics CLI](../topics)
