# Command Line Interface

Fluvio Command Line Interface (aka CLI) is the primary communication mechanism for a Fluvio cluster.

A cluster consists of two components, which are run using the respective commands:
* Streaming Controller (fluvio run sc)
* Streaming Processing Unit (fluvio run spu)

#### Profiles
Fluvio CLI uses Profile files to store most common parameters. This approach offers administrators the convenience to communicate with SC, SPU, or a Kafka environment.


## Produce CLI

Produce CLI can ingest one or more log records in a single session. Topic and Partitions are mandatory parameters, others are optional.

```
Write log records to a topic/partition

USAGE:
    fluvio produce [OPTIONS] --partition <integer> --topic <string>

FLAGS:
    -h, --help    Prints help information

OPTIONS:
    -t, --topic <string>                Topic name
    -p, --partition <integer>           Partition id
    -l, --record-per-line <filename>    Each line is a Record    
    -r, --record-file <filename>...     Entire file is a Record (multiple)
    -c, --sc <host:port>                Address of Streaming Controller
    -s, --spu <host:port>               Address of Streaming Processing Unit
    -k, --kf <host:port>                Address of Kafka Controller
    -e, --profile <profile>             Profile name
```

Produce Topic/Partition command should be sent to SC to look-up the location of SPU that hosts the leader. Log records are then sent directly to the SPU.

Produce command can also send log messages to Kafka. Choose any Broker address and the system will identify the Broker that hosts the leader and forwards log record accordingly.

Log records sent directly to the SPU and are accepted if SPU is the leader for Topic/Partition; rejected otherwise.


#### Interactive CLI

 Produce command called with Topic/Partition opens an interactive session, where each line is interpreted as a record:

```
> fluvio produce -t topic-1 -p 0
line 1
> Ok!
line 2
> Ok!
<Ctrl>-C or <Ctrl>-D to exit (interactive session)
```

#### File Records

Fluvio Produce can ingest log records from files as follows:
* Record per File
* Record per Line 

Record encapsulation is important for binary or JSON objects where the object must be stored in its entirety to be interpreted correctly by the Consumer.


#### Record per File

Use ***record-file*** parameter to ingest an entire file in a single record. One or more files may be sent in a single instance.

```
> fluvio produce -t topic-1 -p 0 -r json-file1.txt -r json-file2.txt
{"name": "john doe"}
> Ok!
{"name": "jane doe"}
> Ok!
```

#### Record per Line

Use ***record-per-line*** parameter to ingest records per line until the end of file is reached. Note, use text-based file to property interpret end of line.

```
> fluvio produce -t topic-1 -p 0 -l my-file.txt
Lorem Ipsum is simply dummy text
> Ok!
Lorem Ipsum has been the industry's standard since 1500's.
> Ok!
It has survived over five centuries
> Ok!
```

