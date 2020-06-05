---
title: Topics
weight: 70
---

The __topic__ is the primary construct for creating a data stream. A topic coupled with a partition creates a unique identifier for a data stream in a Fluvio deployment. The __topic/partition__ unique identifier is used by the __Producers__ and the __Consumers__ to exchange messages over data streams.  

Topics module defines the following CLI operations: 

{{< fluvio >}}
fluvio topic <SUBCOMMAND>

SUBCOMMANDS:
    create      Create a topic
    delete      Delete a topic
    describe    Show details of a topic
    list        Show all topics
{{< /fluvio >}}


## Create Topic

Create __topic__ operation adds a topic to a __Fluvio__ or a __Kafka__ deployment. 

{{< fluvio >}}
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
{{< /fluvio >}}

The flags and options are defined as follows:

* <strong>{{< pre >}}--ignore-rack-assignment{{< /pre >}}</strong>: 
[rack](create_topic) labels are optional parameters assigned to SPUs. The rack label is used during replica assignment to compute rack-aware replica distribution. Apply *ignore_rack_assignment* flag to skip rack-aware assignment. If the SPUs do not have rack labels assigned, the flag is ignored.

* <strong>{{< pre >}}--validate-only{{< /pre >}}</strong>: 
allows you to check if the configuration is correct without applying the changes to the system. This flag is particularly useful to ensure a custom {{< pre >}}--replica-assignment{{< /pre >}} is correct before applying it to the system.

* <strong>{{< pre >}}--topic &lt;string&gt;{{< /pre >}}</strong>:
is the name of the topic to be created. Topic is a mandatory option and it must be unique.

* <strong>{{< pre >}}--partitions &lt;integer&gt;{{< /pre >}}</strong>:
is the number of partitions for the topic. Partitions is mandatory but mutually exclusive with *replica-assigment*. It must be a number greater than 0.

* <strong>{{< pre >}}--replication &lt;integer&gt;{{< /pre >}}</strong>:
is the replication factor for the topic. Replication is mandatory but mutually exclusive with *replica-assigment*. For example, a replication factor of 3 ensures that each message is replicated to 3 different SPUs. Replication must be a number greater than 0.

* <strong>{{< pre >}}--replica-assignment &lt;file.json&gt;{{< /pre >}}</strong>:
is the custom-defined replica assignment file. Replica-assignment is mutually exclusive with *partitions* and *replication*. The replica assignment file allows you to replace Fluvio's built-in replication algorithm with your custom SPU map for each topic/partitions.  

    The replica-assignment JSON file has the following syntax:

    {{< code lang="json" style="light" >}}
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
{{< /code >}}

    The following example shows a replica assignment file with 2 partitions and 3 replicas: 

    {{< code lang="json" style="light" >}}
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
{{< /code >}}

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
is the public interface of the Streaming Controller. The SC is optional and mutually exclusive with {{< pre >}}--kf{{< /pre >}}. The SC is used in combination with [CLI Profiles]({{< relref "overview#profiles" >}}) to compute a target service.

* <strong>{{< pre >}}--kf &lt;host:port&gt;{{< /pre >}}</strong>:
is the public interface of the Kafka Controller. The KF is optional and mutually exclusive with {{< pre >}}--sc{{< /pre >}}. The KF is used in combination with [CLI Profiles]({{< relref "overview#profiles" >}}) to compute a target service.

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
is the custom-defined profile file. The profile is an optional field used to compute a target service. For additional information, see [Target Service]({{< relref "overview#target-service" >}}) section.

### Create Topic Examples 

#### Create a Fluvio Topic

Create a topic with 2 partitions and 3 replica in a Fluvio deployment:

{{< fluvio >}}
$ fluvio topic create --topic sc-topic --partitions 2 --replication 3  --sc `SC`:9003
topic 'sc-topic' created successfully
{{< /fluvio >}}

#### Create a Kafka Topic

Create the a similar topic in a Kafka deployment:

{{< fluvio >}}
$ fluvio topic create --topic kf-topic --partitions 2 --replication 3  --kf 0.0.0.0:9092
topic 'kf-topic' created successfully
{{< /fluvio >}}


## Delete Topic

Delete __topic__ operation deletes a topic from a __Fluvio__ or a __Kafka__ deployment. 

{{< fluvio >}}
fluvio topic delete [OPTIONS] --topic <string>

OPTIONS:
    -t, --topic <string>       Topic name
    -c, --sc <host:port>       Address of Streaming Controller
    -k, --kf <host:port>       Address of Kafka Controller
    -P, --profile <profile>    Profile name
{{< /fluvio >}}

The options are defined as follows:

* <strong>{{< pre >}}--topic &lt;string&gt;{{< /pre >}}</strong>:
is the name of the topic to be deleted. Topic is a mandatory option.

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)

* <strong>{{< pre >}}--kf &lt;host:port&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)


### Delete Topic Examples 

#### Delete a Fluvio Topic

Delete a Fluvio topic:

{{< fluvio >}}
$ fluvio topic delete --topic sc-topic --sc `SC`:9003
topic 'sc-topic' deleted successfully
{{< /fluvio >}}


#### Delete a Kafka Topic

Delete a Kafka topic:

{{< fluvio >}}
$ fluvio topic delete --topic kf-topic  --kf 0.0.0.0:9092
topic 'kf-topic' deleted successfully
{{< /fluvio >}}


## Describe Topics

Describe __topics__ operation show one or more a topics in a __Fluvio__ or a __Kafka__ deployment. 

{{< fluvio >}}
fluvio topic describe [OPTIONS]

OPTIONS:
    -t, --topics <string>...   Topic names
    -c, --sc <host:port>       Address of Streaming Controller
    -k, --kf <host:port>       Address of Kafka Controller
    -P, --profile <profile>    Profile name
    -O, --output <type>        Output [possible values: table, yaml, json]
{{< /fluvio >}}

The options are defined as follows:

* <strong>{{< pre >}}--topics &lt;string&gt;{{< /pre >}}</strong>:
are the names of the topics to be described. A list of one or more topic names is required.

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)

* <strong>{{< pre >}}--kf &lt;host:port&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)

* <strong>{{< pre >}}--output &lt;type&gt;{{< /pre >}}</strong>:
is the format to be used to display the topics. The output is an optional field and it defaults to __table__ format. Alternative formats are: __yaml__ and __json__.


### Describe Topics Examples 

Use __Fluvio CLI__ describe a __Fluvio__ and a __Kafka__ topic.

#### Describe Fluvio Topics

Describe a Fluvio topic in a human-readable format:

{{< fluvio >}}
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
{{< /fluvio >}}

Describe the same topic in _yaml__ format:

{{< fluvio >}}
$ fluvio topic describe --topic sc-topic --sc `SC`:9003 -O yaml
---
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
{{< /fluvio >}}

#### Describe Kafka Topics

Describe a Kafka topic in a human-readable format:

{{< fluvio >}}
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
{{< /fluvio >}}

Describe the same topic in __json__ format:

{{< cli json >}}
$ fluvio topic describe --topic kf-topic  --kf 0.0.0.0:9092 -O json
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
{{< /fluvio >}}

## List Topics

List __topics__ operation shows all topics in a __Fluvio__ or a __Kafka__ deployment. 

{{< fluvio >}}
fluvio topic list [OPTIONS]

OPTIONS:
    -c, --sc <host:port>       Address of Streaming Controller
    -k, --kf <host:port>       Address of Kafka Controller
    -P, --profile <profile>    Profile name
    -O, --output <type>        Output [possible values: table, yaml, json]
{{< /fluvio >}}

The options are defined as follows:

* <strong>{{< pre >}}--sc &lt;host:port&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)

* <strong>{{< pre >}}--kf &lt;host:port&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)

* <strong>{{< pre >}}--profile &lt;profile&gt;{{< /pre >}}</strong>:
See [Create Topic](#create-topic)

* <strong>{{< pre >}}--output &lt;type&gt;{{< /pre >}}</strong>:
See [Describe Topics](#describe-topics)


### List Topics Examples 

#### List Fluvio Topics

List a Fluvio topic in table format:

{{< fluvio >}}
$ fluvio topic list --sc `SC`:9003
 NAME      TYPE      PARTITIONS  REPLICAS  IGNORE-RACK  STATUS       REASON 
 my-topic  computed      1          3           -       provisioned   
 sc-topic  computed      2          3           -       provisioned   
{{< /fluvio >}}

#### List Kafka Topics

List a Kafka topic in table format:

{{< fluvio >}}
$ fluvio topic list --kf 0.0.0.0:9092
 NAME      INTERNAL  PARTITIONS  REPLICAS 
 kf-topic   false        2          3 
{{< /fluvio >}}


{{< links "Related Topics" >}}
* [Produce CLI]({{< relref "produce" >}})
* [Consume CLI]({{< relref "consume" >}})
* [SPUs CLI]({{< relref "spus" >}})
* [Custom SPU CLI]({{< relref "custom-spus" >}})
* [SPU-Groups CLI]({{< relref "spu-groups" >}})
{{< /links >}}