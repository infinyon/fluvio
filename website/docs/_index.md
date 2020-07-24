---
title: Command Line Interface (CLI)
folder: CLI 
menu: Overview
toc: true
weight: 30
---


Fluvio **Command Line Interface (CLI)** is the management tool utilized to provision and monitor Fluvio clusters. The **CLI** uses **profiles** to manage multiple Fluvio clusters. 

## Download and Install

CLI **binaries** are available for download in <a href="https://github.com/infinyon/fluvio/releases" target="_blank">github</a>:

* <a href="https://github.com/infinyon/fluvio/releases/latest" target="_blank">Mac binary</a>
* <a href="https://github.com/infinyon/fluvio/releases/latest" target="_blank">Linux binary</a>

Copy binary to your bin path and make it executable. 

To check CLI version, run:

```bash
$ fluvio version

Fluvio version : 0.6.0
Git Commit     : 805128d24aeb1c728e74e53f318553ea2e37d032
OS Details     : Linux 5.7.8-100.fc31.x86_64 GNU/Linux
Rustc Version  : 1.45.0-nightly (47c3158 2020-06-04)
```

## CLI Overview

Fluvio CLI hierarchy has the following pattern: **fluvio**, **module**, **operation** followed by _options_ and _flags_. 

```bash
fluvio module operation [FLAGS] [OPTIONS]
```

Depending on context, _options_ can be mandatory or optional. Mandatory options are shown in the CLI usage line. For example, in _fluvio topic create_ :

```bash
fluvio topic create [FLAGS] [OPTIONS] <topic-name>
```

* **options**: &dash;&dash;topic-name is mandatory

### Modules

Command line help is available at any level by appending -h or &dash;&dash;help to the command. At top level, you can run **fluvio** with without arguments to get a list of available options.

```bash
$ fluvio 

Fluvio Command Line Interface

fluvio <SUBCOMMAND>

FLAGS:
    -h, --help    Prints help information

SUBCOMMANDS:
    consume       Read messages from a topic/partition
    produce       Write messages to a topic/partition
    spu           SPU operations
    spu-group     SPU group operations
    custom-spu    Custom SPU operations
    topic         Topic operations
    partition     Partition operations
    profile       Profile operations
    cluster       Cluster operations
    version       Prints the current fluvio version information
    help          Prints this message or the help of the given subcommand(s)
```

Top level fluvio CLI is organized by modules:

* spu
* topic
* partitions
* ...

However, there are a few exceptions:

* consume
* produce


"Consume/Produce" are kept at top level as they frequently used operations and we chose convenience over convention.

### Operations

**Operations** describe module capabilities. For example, _topic_ module has the ability to create, list, describe, or delete topics:

```bash
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
```

Other modules, such as **spu** have different options, hence different capabilities.

### Options / Flags

**Options** are module attributes, such as: -p, &dash;&dash;partitions, followed by modifiers, whereas **flags** are attributes without value.

Mandatory options are shown in the syntax definition. All other flags and options are optional.

```bash
$ fluvio topic create --help
Create a topic

fluvio topic create [FLAGS] [OPTIONS] <topic-name>

FLAGS:
    -i, --ignore-rack-assignment    Ignore racks while computing replica assignment
    -d, --dry-run                   Validates configuration, does not provision
        --tls                       Enable TLS
        --enable-client-cert        TLS: use client cert
    -h, --help                      Prints help information

OPTIONS:
    -p, --partitions <partitions>           Number of partitions [default: 1]
    -r, --replication <integer>             Replication factor per partition [default: 1]
    -f, --replica-assignment <file.json>    Replica assignment file
    -c, --cluster <host:port>               Address of cluster
        --domain <domain>                   Required if client cert is used
        --client-cert <client-cert>         Path to TLS client certificate
        --client-key <client-key>           Path to TLS client private key
        --ca-cert <ca-cert>                 Path to TLS ca cert, required when client cert is enabled
    -P, --profile <profile>                 

ARGS:
    <topic-name>    Topic name

```

A small subset of the options, &dash;&dash;sc, and &dash;&dash;profile, are applied to every command. The purpose of these options is to help the CLI identify the location of the services where to send the command.

### Fluvio Clusters

The **CLI** generates commands for a specific **cluster**. The **cluster** is explicit when defined through the an  _option_ or implicit when derived from a _profile_. **CLI** _options_ has higher precedence than _profiles_. 

For additional information, checkout [Fluvio Profiles](./profiles) section.


#### Related Topics
-------------------
* [Fluvio Profiles](./profiles/)
* [Produce CLI](./produce/)
* [Consume CLI](./consume/)
* [SPUs CLI](./spus/)
* [Custom SPU CLI](./custom-spus/)
* [SPU-Groups CLI](./spu-groups/)
* [Topics CLI](./topics/)
