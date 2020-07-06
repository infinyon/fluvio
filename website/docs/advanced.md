---
title: Advanced
toc: true
weight: 100
---

**Advanced** section is **experimental** and requires deep understanding of the message exchanges between system components. For example, create a **Kafka** JSON request API to test **Kafka** response.

Advanced section defines the following CLI operations:

```bash
fluvio advanced <SUBCOMMAND>

SUBCOMMANDS:
    generate    Generate a request template
    run         Send request to server
```

## Generate Kafka Requests

Generate request operation creates a custom JSON template for a specific request. The template needs to be filled-in with additional information for a proper request. 

```bash
fluvio advanced generate --request <>

OPTIONS:
    -r, --request <>    Request API [possible values: ApiVersions,                        
                        ListOffset, Metadata, 
                        LeaderAndIsr, FindCoordinator,
                        JoinGroup, SyncGroup, LeaveGroup, 
                        ListGroups, DescribeGroups, DeleteGroups, 
                        Heartbeat, OffsetFetch]
```

* **&dash;&dash;request &lt;&gt;**:
is a list of requests supported by the CLI. Request is a mandatory option.

-> **Fluvio** has a built-in code generator to create a **Rust API** interface for **Kafka** interoperability. [Rust API for Kafka](https://github.com/infinyon/fluvio/tree/master/kf-protocol/kf-protocol-message/src/kf_code_gen) can be used as reference to create JSON requests.

### Generate Kafka Request Example

... Fluvio


## Run Kafka Requests

Run request operation with a properly formatted JSON file against a Kafka server. 

```bash
fluvio advanced run [OPTIONS] --json-file <file.json> --request <>

OPTIONS:
    -r, --request <>              Request API [possible values: ApiVersions,
                                  ListOffset, Metadata, 
                                  LeaderAndIsr, FindCoordinator,
                                  JoinGroup, SyncGroup, LeaveGroup, 
                                  ListGroups, DescribeGroups, DeleteGroups, 
                                  Heartbeat, OffsetFetch]
    -k, --kf <host:port>          Address of Kafka Controller
    -j, --json-file <file.json>   Request details file
    -P, --profile <profile>       Profile name
```

The options are defined as follows:

* **&dash;&dash;request &lt;&gt;**:
is a list of requests supported by the CLI. Request is a mandatory option.

* **&dash;&dash;kf &lt;host:port&gt;**:
is the interface of the Kafka Controller. The Kf is an optional field used in combination with [Cli Profiles](../#profiles) to compute a target service.

* **&dash;&dash;json-file &lt;file.json&gt;**:
is the JSON file for the request type. JSON file is a mandatory option.

* **&dash;&dash;profile &lt;profile&gt;**:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service](..#target-service) section.
