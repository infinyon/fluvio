---
title: Fluvio CLI
menu: Overview
weight: 10
---

Fluvio **Command Line Interface (CLI)** is the management tool utilized to provision and monitor Fluvio clusters. The **CLI** uses **profiles** to manage multiple Fluvio clusters. 

## Download and Install

CLI **binaries** are available for download in {{< target-blank title="github" url="https://github.com/infinyon/fluvio/releases" >}}:

* {{< target-blank title="Mac binary" url="https://github.com/infinyon/fluvio/releases" >}}
* {{< target-blank title="Linux binary" url="https://github.com/infinyon/fluvio/releases" >}}

Copy the binary to your bin path and make it executable. 

To check CLI version, run:

{{< fluvio >}}
$ fluvio --version
 fluvio 0.3.0
{{< /fluvio >}}

## CLI Overview

Fluvio CLI hierarchy has the following pattern: __fluvio__, __module__, __operation__ followed by _options_ and _flags_. 

{{< fluvio >}}
fluvio module operation [FLAGS] [OPTIONS]
{{< /fluvio >}}

Depending on context, _options_ can be mandatory or optional. Mandatory options are shown in the CLI usage line. For example, in _fluvio topic create_ :

{{< fluvio >}}
fluvio topic create --partitions <integer> --replication <integer> --topic <string>
{{< /fluvio >}}

options: {{< pre >}}--topic{{< /pre >}}, {{< pre >}}--partitions{{< /pre >}}, and {{< pre >}}--replication{{< /pre >}}, are mandatory.

### Modules

Command line help is available at any level by appending {{< pre >}}-h{{< /pre >}} or {{< pre >}}--help{{< /pre >}} to the command. At top level, you can run __fluvio__ with without arguments to get a list of available options.

{{< fluvio >}}
$ fluvio 
Fluvio Command Line Interface

fluvio <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    consume       Read messages from a topic/partition
    produce       Write log records to a topic/partition
    spu           SPU Operations
    spu-group     SPU Group Operations
    custom-spu    Custom SPU Operations
    topic         Topic operations
    advanced      Advanced operations
    help          Prints this message or the help of the given subcommand(s)
{{< /fluvio >}}

Top level fluvio CLI is organized by modules:

* spu
* spu-group
* custom-spu
* topic

However, there are a few exceptions:

* consume/produce
* advanced

"Consume/Produce" are kept at top level as they frequently used operations and we chose convenience over convention. "Advanced" is an aggregate of system-wide operations that don't belong to any particular module.

### Operations

**Operations** describe module capabilities. For example, _topic_ module has the ability to create, list, describe, or delete topics:

{{< fluvio >}}
$ fluvio topic
Topic operations

fluvio topic <SUBCOMMAND>

FLAGS:
    -h, --help    Prints help information

SUBCOMMANDS:
    create      Create a topic
    delete      Delete a topic
    describe    Show details of a topic
    list        Show all topics
    help        Prints this message or the help of the given subcommand(s)
{{< /fluvio >}}

Other modules, such as __spu__ have different options, hence different capabilities.

### Options / Flags

**Options** are module attributes, such as: {{< pre >}}-t, --topic{{< /pre >}}, followed by modifiers, whereas **flags** are attributes without value.

Mandatory options are shown in the syntax definition. All other flags and options are optional.

{{< fluvio >}}
$ fluvio topic create --help
Create a topic

fluvio topic create [FLAGS] [OPTIONS] --partitions <integer> --replication <integer> --topic <string>

FLAGS:
    -i, --ignore-rack-assignment    Ignore racks while computing replica assignment
    -v, --validate-only             Validates configuration, does not provision
    -h, --help                      Prints help information

OPTIONS:
    -t, --topic <string>                    Topic name
    -p, --partitions <integer>              Number of partitions
    -r, --replication <integer>             Replication factor per partition
    -f, --replica-assignment <file.json>    Replica assignment file
    -c, --sc <host:port>                    Address of Streaming Controller
    -k, --kf <host:port>                    Address of Kafka Controller
    -P, --profile <profile>                 Profile name
{{< /fluvio >}}

A small subset of the options, {{< pre >}}--kf, --sc,{{< /pre >}} and {{< pre >}}--profile{{< /pre >}}, are applied to every command. The purpose of these options is to help the CLI identify the location of the services where to send the command.

### Fluvio Clusters

The **CLI** generates commands for a specific **cluster**. The **cluster** is explicit when defined through the an  _option_ or implicit when derived from a _profile_. **CLI** _options_ has higher precedence than _profiles_. 

For additional information, checkout [Fluvio Profiles]({{< relref "profiles" >}}) section.


{{< links "Related Topics" >}}
* [Fluvio Profiles]({{< relref "profiles" >}})
* [Produce CLI]({{< relref "produce" >}})
* [Consume CLI]({{< relref "consume" >}})
* [SPUs CLI]({{< relref "spus" >}})
* [Custom SPU CLI]({{< relref "custom-spus" >}})
* [SPU-Groups CLI]({{< relref "spu-groups" >}})
* [Topics CLI]({{< relref "topics" >}})
{{< /links >}}
