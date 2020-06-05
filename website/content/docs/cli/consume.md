---
title: Consume
weight: 40
---


The __Consumer__ is responsible for reading messages from data streams in a __Fluvio__ or a __Kafka__ deployment. The messages are written to the topic/partitions by the __Producers__.


## Consume Messages

__Consume__ command can operate in two modes:

* __read-one__,
* __read-continuously__.

__Consume__ CLI command has the following operations: 

{{< fluvio >}}
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
{{< /fluvio >}}

The flags and options are defined as follows:

* <strong>{{< pre >}}--from-beginning{{< /pre >}}</strong>: is a flag that instructs the SPU to read from the beginning of the topic/partition. This is an optional flag; if blank the CLI will wait for the __Producer__ to write to the data stream.

* <strong>{{< pre >}}--continuous{{< /pre >}}</strong>: is a flag that instructs the CLI to read from the data stream in an infinite loop. Press Ctrl-C, or send SIGINT, to exit loop.

* <strong>{{< pre >}}--suppress-unknown{{< /pre >}}</strong>: is a flag that instructs the CLI to skip messages that were not parsed correctly. Suppress-unknown is used with data streams that contain messages with mixed types where some messages cannot be successfully parsed. This is an optional flag.

* <strong>{{< pre >}}--topic &lt;string&gt;{{< /pre >}}</strong>:
is the name of the topic from which to read the messages. The topic is a mandatory option and it is used in combination with {{< pre >}}--partition{{< /pre >}} to uniquely identify a data stream.

* <strong>{{< pre >}}--partition &lt;integer&gt;{{< /pre >}}</strong>:
is the partition index of a topic from which to read the messages. The partition is a mandatory option and it is used in combination with {{< pre >}}--topic{{< /pre >}} to uniquely identify a data stream.

* <strong>{{< pre >}}--maxbytes &lt;integer&gt;{{< /pre >}}</strong>:
is the maximum number of bytes of a message retrieved. The maxbytes field is optional.

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
is the public interface of the Streaming Controller. The SC is optional and mutually exclusive with {{< pre >}}--spu{{< /pre >}} and {{< pre >}}--kf{{< /pre >}}. The SC is used in combination with [CLI Profiles]({{< relref "overview#profiles" >}}) to compute a target service.

* <strong>{{< pre >}}--spu &lt;host:port&gt;{{< /pre >}}</strong>:
is the public interface of the Streaming Processing Unit. The SPU is optional and mutually exclusive with {{< pre >}}--sc{{< /pre >}} and {{< pre >}}--kf{{< /pre >}}. The SPU is used in combination with [CLI Profiles]({{< relref "overview#profiles" >}}) to compute a target service.

* <strong>{{< pre >}}--kf &lt;host:port&gt;{{< /pre >}}</strong>:
is the public interface of the Kafka Controller. The KF is optional and mutually exclusive with {{< pre >}}--sc{{< /pre >}} and {{< pre >}}--spu{{< /pre >}}. The KF is used in combination with [CLI Profiles]({{< relref "overview#profiles" >}}) to compute a target service.

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service]({{< relref "overview#target-service" >}}) section.

* <strong>{{< pre >}}--output &lt;type&gt;{{< /pre >}}</strong>:
is the format to be used to display the messages. The output is an optional field and it defaults to __dynamic__, where the parser will attempt to guess the message type. Known formats are: __text__, __binary__, __json__, and __raw__.


### Consume Messages Examples 

#### Consume Messages from Fluvio SC

Consume all _my-topic_  messages from the beginning of the Fluvio SC queue:

{{< fluvio >}}
$ fluvio consume -t my-topic -p 0 --sc `SC`:9003 -g
hello world
test
hello World!
one 
two
three
{{< /fluvio >}}


#### Consume Messages for Kafka

Consume all _kf-topic_  messages from the beginning of the Kafka queue:

{{< fluvio >}}
$ fluvio consume -t kf-topic -p 0 --kf 0.0.0.0:9092 -g
Hello World
one
two
three
^C
{{< /fluvio >}}



{{< links "Related Topics" >}}
* [Produce CLI]({{< relref "produce" >}})
* [SPUs CLI]({{< relref "spus" >}})
* [Custom SPU CLI]({{< relref "custom-spus" >}})
* [SPU-Groups CLI]({{< relref "spu-groups" >}})
* [Topics CLI]({{< relref "topics" >}})
{{< /links >}}