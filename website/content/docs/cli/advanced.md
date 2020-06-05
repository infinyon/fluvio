---
title: Advanced
weight: 100
---

__Advanced__ section is __experimental__ and requires deep understanding of the message exchanges between system components. For example, create a __Kafka__ JSON request API to test __Kafka__ response.

Advanced section defines the following CLI operations:

{{< fluvio >}}
fluvio advanced <SUBCOMMAND>

SUBCOMMANDS:
    generate    Generate a request template
    run         Send request to server
{{< /fluvio >}}

## Generate Kafka Requests

Generate request operation creates a custom JSON template for a specific request. The template needs to be filled-in with additional information for a proper request. 

{{< fluvio >}}
fluvio advanced generate --request <>

OPTIONS:
    -r, --request <>    Request API [possible values: ApiVersions,                        
                        ListOffset, Metadata, 
                        LeaderAndIsr, FindCoordinator,
                        JoinGroup, SyncGroup, LeaveGroup, 
                        ListGroups, DescribeGroups, DeleteGroups, 
                        Heartbeat, OffsetFetch]
{{< /fluvio >}}

* <strong>{{< pre >}}--request &lt;&gt;{{< /pre >}}</strong>:
is a list of requests supported by the CLI. Request is a mandatory option.

    {{< idea >}}
__Fluvio__ has a built-in code generator to create a __Rust API__ interface for __Kafka__ interoperability. [Rust API for Kafka](https://github.com/infinyon/fluvio/tree/master/kf-protocol/kf-protocol-message/src/kf_code_gen) can be used as reference to create JSON requests.
{{< /idea >}}

### Generate Kafka Request Example

... Fluvio


## Run Kafka Requests

Run request operation with a properly formatted JSON file against a Kafka server. 

{{< fluvio >}}
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
{{< /fluvio >}}

The options are defined as follows:

* <strong>{{< pre >}}--request &lt;&gt;{{< /pre >}}</strong>:
is a list of requests supported by the CLI. Request is a mandatory option.

* <strong>{{< pre >}}--kf &lt;host:port&gt;{{< /pre >}}</strong>:
is the interface of the Kafka Controller. The Kf is an optional field used in combination with [CLI Profiles]({{< relref "overview#profiles" >}}) to compute a target service.

* <strong>{{< pre >}}--json-file &lt;file.json&gt;{{< /pre >}}</strong>:
is the JSON file for the request type. JSON file is a mandatory option.

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service]({{< relref "overview#target-service" >}}) section.

... Fluvio

