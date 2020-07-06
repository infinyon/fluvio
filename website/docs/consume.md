---
title: Consume
toc: true
weight: 40
---


The **Consumer** is responsible for reading messages from data streams in a **Fluvio** or a **Kafka** deployment. The messages are written to the topic/partitions by the **Producers**.


## Consume Messages

**Consume** command can operate in two modes:

* **read-one**,
* **read-continuously**.

**Consume** CLI command has the following operations: 

```bash
fluvio consume [FLAGS] [OPTIONS] --partition <integer> --topic <string>

FLAGS:
    -g, --from-beginning      Start reading from this offset
    -C, --continuous          Read messages in a infinite loop
    -s, --suppress-unknown    Suppress items items that have an unknown output type

OPTIONS:
    -t, --topic <string>         Topic name
    -p, --partition <integer>    Partition id
    -b, --maxbytes <integer>     Maximum number of bytes to be retrieved
    -c, --sc <host:port>         Address of Streaming Controller
    -u, --spu <host:port>        Address of Streaming Processing Unit
    -k, --kf <host:port>         Address of Kafka Controller
    -P, --profile <profile>      Profile name
    -O, --output <type>          Output [possible values: dynamic, text, binary, json, raw]
```

The flags and options are defined as follows:

* **&dash;&dash;from-beginning**:
is a flag that instructs the SPU to read from the beginning of the topic/partition. This is an optional flag; if blank the CLI will wait for the **Producer** to write to the data stream.

* **&dash;&dash;continuous**:
is a flag that instructs the CLI to loop forever and display messages as they arrive. Press Ctrl-C, or send SIGINT, to exit loop.

* **&dash;&dash;suppress-unknown**:
is a flag that instructs the CLI to skip messages that were not parsed correctly. Suppress-unknown is used with data streams that contain messages with mixed types where some messages cannot be successfully parsed. This is an optional flag.

* **&dash;&dash;topic &lt;string&gt;**:
is the name of the topic from which to read the messages. The topic is a mandatory option and it is used in combination with &dash;&dash;partition to uniquely identify a data stream.

* **&dash;&dash;partition &lt;integer&gt;**:
is the partition index of a topic from which to read the messages. The partition is a mandatory option and it is used in combination with &dash;&dash;topic to uniquely identify a data stream.

* **&dash;&dash;maxbytes &lt;integer&gt;**:
is the maximum number of bytes of a message retrieved. The maxbytes field is optional.

* **&dash;&dash;sc &lt;host:port&gt;**:
is the public interface of the Streaming Controller. The SC is optional and mutually exclusive with &dash;&dash;spu and &dash;&dash;--kf. The SC is used in combination with [Cli Profiles](../profiles) to compute a target service.

* **&dash;&dash;spu &lt;host:port&gt;**:
is the public interface of the Streaming Processing Unit. The SPU is optional and mutually exclusive with &dash;&dash;sc and &dash;&dash;kf. The SPU is used in combination with [Cli Profiles](../profiles) to compute a target service.

* **&dash;&dash;kf &lt;host:port&gt;**:
is the public interface of the Kafka Controller. The KF is optional and mutually exclusive with &dash;&dash;sc and &dash;&dash;spu. The KF is used in combination with [Cli Profiles](../profiles) to compute a target service.

* **&dash;&dash;profile &lt;profile&gt;**:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service](..#target-service) section.

* **&dash;&dash;output &lt;type&gt;**:
is the format to be used to display the messages. The output is an optional field and it defaults to **dynamic**, where the parser will attempt to guess the message type. Known formats are: **text**, **binary**, **json**, and **raw**.


### Consume Messages Examples 

#### Consume Messages from Fluvio SC

Consume all _my-topic_  messages from the beginning of the Fluvio SC queue:

```bash
$ fluvio consume -t my-topic -p 0 --sc `SC`:9003 -g
hello world
test
hello World!
one 
two
three
```


#### Consume Messages for Kafka

Consume all _kf-topic_  messages from the beginning of the Kafka queue:

```bash
$ fluvio consume -t kf-topic -p 0 --kf 0.0.0.0:9092 -g
Hello World
one
two
three
^C
```



#### Related Topics
-------------------
* [Produce CLI](../produce/)
* [SPUs CLI](../spus/)
* [Custom SPU CLI](../custom-spus/)
* [SPU-Groups CLI](../spu-groups/)
* [Topics CLI](../topics/)
