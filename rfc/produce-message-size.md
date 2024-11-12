# Enhancing Fluvio Message Sizes

This RFC proposes modifications to Fluvio's handling of message sizes. 

## Introduction

`batch_size` producer config must not reject large records, just send them directly.

Create a new `max_request_size` producer config that must reject large messages.
I am using `max_request_size` because Kafka uses `max.request.size` but we can change it to other config name.

Compression sizes should not be used for these producer configs.

## Proposed Enhancements

1. Handling Larger Messages than Batch Size

If a single record exceeds the defined `batch_size`, Fluvio will process the record in a new batch without the `batch_size` as limit, ensuring that larger messages are not discarded or delayed. If the record does not exceed the `batch_size`, Fluvio will process the record as part of an already existing batch or create a new one if the batch is full.

2. Handling Larger Messages than the Max Request Size

Fluvio will have a new configuration parameter, max_request_size, that will define the maximum size of a request that can be sent by the producer. This configuration will make Fluvio display errors when a message exceeds the defined `max_request_size`, even if it's a message with only one record or a batch of records.

3. Compression Behavior

Fluvio will ensure that configuration limits that use size constraints, such as `batch_size` and `max_request_size` will only use the uncompressed message size.
It affects the size of messages in transit but doesn't change the maximum request size constraints.


## Fluvio CLI

Preparing the environment, with a topic and a large data file:

```bash
fluvio topic create large-data-topic
printf 'This is a sample line. ' | awk -v b=500000 '{while(length($0) < b) $0 = $0 $0}1' | cut -c1-500000 > large-data-file.txt
```

### Batch Size

`batch_size` will define the maximum size of a batch of records that can be sent by the producer. If a record exceeds this size, Fluvio will process the record in a new batch without the `batch_size` as limit.

```bash
fluvio produce large-data-topic --batch-size 16536 --file large-data-file.txt --raw
```

There will not be any errors displayed, even if the message exceeds the batch size. But the record will be processed as a new batch.

### Max Request Size

`max_request_size` will define the maximum size of a message that can be sent by the producer. If a message exceeds this size, Fluvio will throw an error. Even if it's a message with only one record or a batch of them.

```bash
fluvio produce large-data-topic --max-request-size 16384 --file large-data-file.txt --raw
```

Will be displayed the following error:

```bash
record size (xyz bytes), exceeded maximum request size (1048576 bytes)
```

### Compression

`batch_size` and `max_request_size` will only use the uncompressed message size.

```bash
fluvio produce large-data-topic --batch-size 16536 --compression gzip --file large-data-file.txt --raw
fluvio produce large-data-topic --max-request-size 16384 --compression gzip --file large-data-file.txt --raw
```

The first one and the second one will use the uncompressed message size to be calculated. Only the second one will display an error because the uncompressed message exceeds the max request size.

## References

### Kafka Behavior

Kafka has a similar behavior for handling larger messages than batch size and max request size.

Preparing the environment, with a topic and a large data file:

```bash
kafka-topics --create --topic large-data-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
printf 'This is a sample line. ' | awk -v b=500000 '{while(length($0) < b) $0 = $0 $0}1' | cut -c1-500000 > large-data-file.txt
```

Producing large messages for the topic with a small batch size will not display any errors.

```bash
kafka-console-producer --topic large-data-topic --bootstrap-server localhost:9092 --producer-property batch.size=16384 < large-data-file.txt
```


Producing large messages for the topic with a small max request size will display an error:

```bash
kafka-console-producer --topic large-data-topic --bootstrap-server localhost:9092 --producer-property max.request.size=16384 < large-data-file.txt
org.apache.kafka.common.errors.RecordTooLargeException: The message is 500087 bytes when serialized which is larger than 16384, which is the value of the max.request.size configuration.
```

Producing large messages to the topic with compression will not use the compression size to calculate the batch size:

```bash
kafka-console-producer --topic large-data-topic --bootstrap-server localhost:9092 --producer-property batch.size=16384 --producer-property compression.type=gzip < large-data-file.txt
kafka-console-producer --topic large-data-topic --bootstrap-server localhost:9092 --producer-property max.request.size=16384 --producer-property compression.type=gzip < large-data-file.txt
```

Both commands will not use the compression size to calculate the batch size and the max request size, respectively. But only the second one will display an error because the message exceeds the max request size.
