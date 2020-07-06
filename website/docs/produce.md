---
title: Produce
toc: true
weight: 30
---

The __Producer__ is responsible for sending messages to data streams in a __Fluvio__ or a __Kafka__ deployment. The messages are placed in topics/partitions that are retrieved by the __Consumers__.


## Produce Messages

__Produce__ command can operate in two modes:

* __send-one__,
* __send-continuously__.

__Produce__ CLI command has the following operations: 

```bash
fluvio produce [FLAGS] [OPTIONS] --partition <integer> --topic <string>

FLAGS:
    -C, --continuous     Send messages in an infinite loop

OPTIONS:
    -t, --topic <string>                Topic name
    -p, --partition <integer>           Partition id
    -l, --record-per-line <filename>    Each line is a Record
    -r, --record-file <filename>...     Entire file is a Record (multiple)
    -c, --sc <host:port>                Address of Streaming Controller
    -u, --spu <host:port>               Address of Streaming Processing Unit
    -k, --kf <host:port>                Address of Kafka Controller
    -P, --profile <profile>             Profile name
```

The flags and options are defined as follows:

* **&dash;&dash;continuous**: 
is a flag that instructs the CLI to read the input in an infinite loop. Press Ctrl-C, or send SIGINT, to exit loop.

* **&dash;&dash;topic &lt;string&gt;**:
is the name of the topic to receive the messages. The topic is a mandatory option and it is used in combination with &dash;&dash;partition to uniquely identify a data stream.

* **&dash;&dash;partition &lt;integer&gt;**:
is the index of a topic to receive the messages. The partition is a mandatory option and it is used in combination with &dash;&dash;topic to uniquely identify a data stream.

* **&dash;&dash;record-per-line &lt;filename&gt;**:
is the file that contains the records to be sent to the topic/partition. Each line consists of one record. This is an optional field.

* **&dash;&dash;record-file &lt;filename&gt;**:
is the file that contains the record to be sent to the topic/partition. The entire file is sent as one record. This field can be used to send binary objects such as images to the data stream. This is an optional field.

* **&dash;&dash;sc &lt;host:port&gt;**:
is the public interface of the Streaming Controller. The SC is optional and mutually exclusive with &dash;&dash;spu and &dash;&dash;kf. The SC is used in combination with [Cli Profiles](../profiles) to compute a target service.

* **&dash;&dash;spu &lt;host:port&gt;**:
is the public interface of the Streaming Processing Unit. The SPU is optional and mutually exclusive with &dash;&dash;sc and &dash;&dash;kf. The SPU is used in combination with [Cli Profiles](../profiles) to compute a target service.

* **&dash;&dash;kf &lt;host:port&gt;**:
is the public interface of the Kafka Controller. The KF is optional and mutually exclusive with &dash;&dash;sc and &dash;&dash;spu. The KF is used in combination with [Cli Profiles](../profiles) to compute a target service.

* **&dash;&dash;profile &lt;profile&gt;**:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service](..#target-service) section.

### Produce Messages Examples 

#### Produce Messages for Fluvio SC

Produce one message to Fluvio SC:

```bash
$ fluvio produce -t my-topic -p 0 --sc `SC`:9003
hello World!
Ok!
```

Continuously produce messages to Fluvio SC:

```bash
$ fluvio produce -t my-topic -p 0 --sc `SC`:9003 -C
one 
Ok!
two
Ok!
three
Ok!
^C
```


#### Produce Messages for Kafka

Produce one message to Kafka:

```bash
$ fluvio produce -t kf-topic -p 0 --kf 0.0.0.0:9092
Hello World
Ok!
```

Continuously produce messages to Kafka:

```bash
$ fluvio produce -t kf-topic -p 0 --kf 0.0.0.0:9092 -C
one
Ok!
two
Ok!
three
Ok!
^C
```


#### Related Topics
-------------------
* [Consume CLI](../consume)
* [SPUs CLI](../spus)
* [Custom SPU CLI](../custom-spus)
* [SPU-Groups CLI](../spu-groups)
* [Topics CLI](../topics)