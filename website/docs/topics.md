---
title: Topics
toc: true
weight: 70
---

The **topic** is the primary construct for creating a data stream. A topic coupled with a partition creates a unique identifier for a data stream in a Fluvio deployment. The **topic/partition** unique identifier is used by the **Producers** and the **Consumers** to exchange messages over data streams.  

Topics module defines the following CLI operations: 

```bash
fluvio topic <SUBCOMMAND>

SUBCOMMANDS:
    create      Create a topic
    delete      Delete a topic
    describe    Show details of a topic
    list        Show all topics
```


## Create Topic

Create **topic** operation adds a topic to a **Fluvio** or a **Kafka** deployment. 

```bash
fluvio topic create [FLAGS] [OPTIONS] --topic <string> --partitions <integer> --replication <integer> 

FLAGS:
    -i, --ignore-rack-assignment    Ignore racks while computing replica assignment
    -v, --validate-only             Validates configuration, does not provision

OPTIONS:
    -t, --topic <string>                    Topic name
    -p, --partitions <integer>              Number of partitions
    -r, --replication <integer>             Replication factor per partition
    -f, --replica-assignment <file.json>    Replica assignment file
    -c, --sc <host:port>                    Address of Streaming Controller
    -k, --kf <host:port>                    Address of Kafka Controller
    -P, --profile <profile>                 Profile name
```

The flags and options are defined as follows:

* **&dash;&dash;ignore-rack-assignment**:
 _rack labels_ are optional parameters assigned to SPUs. The rack label is used by [replica assignment](/docs/architecture/replication/#replica-assignment-algorithm) to compute rack-aware replica distribution. When **ignore_rack_assignment** flag is set, replica assignment ignores rack labels. If the SPUs do not have rack labels assigned, the flag is ignored.

* **&dash;&dash;validate-only**:
allows you to check if the configuration is correct without applying the changes to the system. This flag is particularly useful to ensure a [manual replica-assignment](/docs/architecture/replication/#manual-replica-assignment) is correct before applying it to the system.

* **&dash;&dash;topic &lt;string&gt;**:
is the name of the topic to be created. Topic is a mandatory option and it must be unique.

* **&dash;&dash;partitions &lt;integer&gt;**:
is the number of partitions for the topic. Partitions is mandatory but mutually exclusive with _&dash;&dash;replica-assigment_. It must be a number greater than 0.

* **&dash;&dash;replication &lt;integer&gt;**:
is the replication factor for the topic. Replication is mandatory but mutually exclusive with _&dash;&dash;replica-assigment_. For example, a replication factor of 3 ensures that each message is replicated to 3 different SPUs. Replication must be a number greater than 0.

* **&dash;&dash;replica-assignment &lt;file.json&gt;**:
is the custom-defined replica assignment file. Replica-assignment is mutually exclusive with *partitions* and *replication*. The replica assignment file allows you to replace Fluvio's built-in replication algorithm with your custom SPU map for each topic/partitions.  

  The replica-assignment JSON file has the following syntax:

  ```bash
  { 
      "partitions": [
          {
              "id": <partition-id>,
              "replicas": [
                  <spu-id>, 
                  ...
              ]
          }, 
          ...
      ]
  }
  ```

  The following example shows a replica assignment file with 2 partitions and 3 replicas: 

  ```json
  {
      "partitions": [
          {
              "id": 0,
              "replicas": [
                  0,
                  2,
                  3
              ]
          },
          {
              "id": 1,
              "replicas": [
                  1,
                  3,
                  2
              ]
          }
      ]
  }
  ```

* **&dash;&dash;sc &lt;host:port&gt;**:
is the public interface of the Streaming Controller. The SC is optional and mutually exclusive with &dash;&dash;kf. The SC is used in combination with [Cli Profiles](../profiles) to compute a target service.

* **&dash;&dash;kf &lt;host:port&gt;**:
is the public interface of the Kafka Controller. The KF is optional and mutually exclusive with&dash;&dash;sc. The KF is used in combination with [Cli Profiles](../profiles) to compute a target service.

* **&dash;&dash;profile &lt;profile&gt;**:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service](..#target-service) section.

### Create Topic Examples 

#### Create a Fluvio Topic

Create a topic with 2 partitions and 3 replica in a Fluvio deployment:

```bash
$ fluvio topic create --topic sc-topic --partitions 2 --replication 3  --sc `SC`:9003
topic 'sc-topic' created successfully
```

#### Create a Kafka Topic

Create the a similar topic in a Kafka deployment:

```bash
$ fluvio topic create --topic kf-topic --partitions 2 --replication 3  --kf 0.0.0.0:9092
topic 'kf-topic' created successfully
```


## Delete Topic

Delete **topic** operation deletes a topic from a **Fluvio** or a **Kafka** deployment. 

```bash
fluvio topic delete [OPTIONS] --topic <string>

OPTIONS:
    -t, --topic <string>       Topic name
    -c, --sc <host:port>       Address of Streaming Controller
    -k, --kf <host:port>       Address of Kafka Controller
    -P, --profile <profile>    Profile name
```

The options are defined as follows:

* **&dash;&dash;topic &lt;string&gt;**:
is the name of the topic to be deleted. Topic is a mandatory option.

* **&dash;&dash;sc &lt;host:port&gt;**:
See [Create Topic](#create-topic)

* **&dash;&dash;kf &lt;host:port&gt;**:
See [Create Topic](#create-topic)

* **&dash;&dash;profile &lt;profile&gt;**:
See [Create Topic](#create-topic)


### Delete Topic Examples 

#### Delete a Fluvio Topic

Delete a Fluvio topic:

```bash
$ fluvio topic delete --topic sc-topic --sc `SC`:9003
topic 'sc-topic' deleted successfully
```


#### Delete a Kafka Topic

Delete a Kafka topic:

```bash
$ fluvio topic delete --topic kf-topic  --kf 0.0.0.0:9092
topic 'kf-topic' deleted successfully
```


## Describe Topics

Describe **topics** operation show one or more a topics in a **Fluvio** or a **Kafka** deployment. 

```bash
fluvio topic describe [OPTIONS]

OPTIONS:
    -t, --topics <string>...   Topic names
    -c, --sc <host:port>       Address of Streaming Controller
    -k, --kf <host:port>       Address of Kafka Controller
    -P, --profile <profile>    Profile name
    -O, --output <type>        Output [possible values: table, yaml, json]
```

The options are defined as follows:

* **&dash;&dash;topic &lt;string&gt;**:
are the names of the topics to be described. A list of one or more topic names is required.

* **&dash;&dash;sc &lt;host:port&gt;**:
See [Create Topic](#create-topic)

* **&dash;&dash;kf &lt;host:port&gt;**:
See [Create Topic](#create-topic)

* **&dash;&dash;profile &lt;profile&gt;**:
See [CLI Profiles](../profiles)

* **&dash;&dash;output &lt;type&gt;**:
is the format to be used to display the topics. The output is an optional field and it defaults to **table** format. Alternative formats are: **yaml** and **json**.


### Describe Topics Examples 

Use **Fluvio CLI** describe a **Fluvio** and a **Kafka** topic.

#### Describe Fluvio Topics

Describe a Fluvio topic in a human-readable format:

```bash
$ fluvio topic describe --topic sc-topic --sc `SC`:9003
 Name                    :  sc-topic 
 Type                    :  computed 
 Partition Count         :  2 
 Replication Factor      :  3 
 Ignore Rack Assignment  :  - 
 Status                  :  provisioned 
 Reason                  :  - 
 Partition Map               
 -----------------           
     ID      LEADER      REPLICAS         LIVE-REPLICAS 
      0        2         [2, 200, 0]      [0, 200] 
      1       200        [200, 1, 2]      [1, 2] 
```

Describe the same topic in **yaml** format:
* **&dash;&dash;output yaml**:

```yaml
- topic_metadata:
    name: sc-topic
    topic:
      type_computed: true
      partitions: 2
      replication_factor: 3
      ignore_rack_assignment: false
      status: Provisioned
      reason: ""
      partition_map:
        - id: 0
          leader: 2
          replicas:
            - 2
            - 200
            - 0
          live_replicas:
            - 0
            - 200
        - id: 1
          leader: 200
          replicas:
            - 200
            - 1
            - 2
          live_replicas:
            - 1
            - 2
```

#### Describe Kafka Topics

Describe a Kafka topic in a human-readable format:

```bash
$ fluvio topic describe --topic kf-topic  --kf 0.0.0.0:9092
 Name                :  kf-topic 
 Internal            :  false 
 Partition Count     :  2 
 Replication Factor  :  3 
 Partition Replicas      
 -----------------       
     ID      STATUS      LEADER      REPLICAS       ISR 
      0        Ok          1         [1, 2, 3]      [1, 2, 3] 
      1        Ok          2         [2, 3, 1]      [2, 3, 1] 
```

Describe the same topic in **json** format:

* **&dash;&dash;output json**:

```json
 [
  {
    "topic_metadata": {
      "name": "kf-topic",
      "topic": {
        "is_internal": false,
        "partitions": 2,
        "replication_factor": 3,
        "partition_map": [
          {
            "id": 0,
            "leader": 1,
            "replicas": [
              1,
              2,
              3
            ],
            "isr": [
              1,
              2,
              3
            ],
            "status": "Ok"
          },
          {
            "id": 1,
            "leader": 2,
            "replicas": [
              2,
              3,
              1
            ],
            "isr": [
              2,
              3,
              1
            ],
            "status": "Ok"
          }
        ]
      }
    }
  }
]
```

## List Topics

List **topics** operation shows all topics in a **Fluvio** or a **Kafka** deployment. 

```bash
fluvio topic list [OPTIONS]

OPTIONS:
    -c, --sc <host:port>       Address of Streaming Controller
    -k, --kf <host:port>       Address of Kafka Controller
    -P, --profile <profile>    Profile name
    -O, --output <type>        Output [possible values: table, yaml, json]
```

The options are defined as follows:

* **&dash;&dash;sc &lt;host:port&gt;**:
See [Create Topic](#create-topic)

* **&dash;&dash;kf &lt;host:port&gt;**:
See [Create Topic](#create-topic)

* **&dash;&dash;profile &lt;profile&gt;**:
See [CLI Profiles](../profiles)

* **&dash;&dash;output &lt;type&gt;**:
See [Describe Topics](#describe-topics)


### List Topics Examples 

#### List Fluvio Topics

List a Fluvio topic in table format:

```bash
$ fluvio topic list --sc `SC`:9003
 NAME      TYPE      PARTITIONS  REPLICAS  IGNORE-RACK  STATUS       REASON 
 my-topic  computed      1          3           -       provisioned   
 sc-topic  computed      2          3           -       provisioned   
```

#### List Kafka Topics

List a Kafka topic in table format:

```bash
$ fluvio topic list --kf 0.0.0.0:9092
 NAME      INTERNAL  PARTITIONS  REPLICAS 
 kf-topic   false        2          3 
```


#### Related Topics
-------------------
* [Produce CLI](../produce)
* [Consume CLI](../consume)
* [SPUs CLI](../spus)
* [Custom SPU CLI](../custom-spus)
* [SPU-Groups CLI](../spu-groups)