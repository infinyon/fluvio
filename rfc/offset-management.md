## Scope

Provide a minimal way to save and recover offset for the consumer. Offset mgt facility provides a way to set TTL for offset lifetime.
We are not considering the consumer group use case as part of this RFC

### Use Case 1
Automatic offset commit for given consumer id and a default pattern of
committing the offset of the previous record on fetching of the next record in a stream.

### Use Case 2
Though offsets management is by default automatic. The Manual Offset api provides
explict offset commit and flush control.

## Offset Management

### Use Case 1: Automatic Offset API

The key operation of the API would be to send consumed offsets associated with a fluvio topic and id for offset tracking.
The offset_start parameter is used to initialize the offset of the given consumer. Subsequent
connections of a consumer with the same `offset_consumer` id will receive offsets from the tracked offset value.

The previous loops offset is automatically synced up on the next `stream.next()` call.

```rust
use fluvio::consumer::ConsumerConfigBuilder;
use fluvio::consumer::OffsetManagement;

async fn offset_example() -> Result<()> {
    let client = fluvio::connect();

   let cfg = ConsumerConfigExtBuilder::new()
           .topic("mytopic")
           .partition(0) // optional, defaults to zero?
           .offset_start(Offset::Beginning)  // optional, defaults to (whatever was before)
           .offset_consumer("my-consumer")    // optional, if set consumer strategy defaults to ::Auto
           .offset_flush(3000)  // optional, defaults to ?
           .max_bytes(12300) // optional, here as an example of other settings
           .build()?;
    let stream = client.consumer_with_config(cfg);

    // when stream.next is called, previous offset will be sent to cluster to commit
    while let Some(Ok(record)) = stream.next().await {
        println!("{}", record.get_value().as_utf8_lossy_string());
    }

    // synchronous flush on stream drop?

    Ok(())
}

```

### Use Case 2: Manual API

This api has the same behavior as the Automatic Offset API with the difference being
that `offset_commit()` and `offset_flush()` calls are required. This allows for
consumption patterns that may need to take in more than one record before committing an offset as consumed.

```rust
use fluvio::consumer::ConsumerConfigBuilder;
use fluvio::consumer::OffsetManagement;

async fn offset_example() -> Result<()> {
    let client = fluvio::connect();
    let cfg = ConsumerConfigExtBuilder::new()
        .topic("mytopic")
        .partition(0) // optional, defaults to zero?
        .offset_start(Offset::Beginnning)  // optional, defaults to (whatever was before)
        .offset_consumer("my-consumer")    // required id
        .offset_strategy(OffsetManagement::Manual) // optional
        .offset_flush(3000)  // Duration, optional defaults to ?
        .max_bytes(12300) // optional, here as an example of other settings
        .build()?;
    let stream = client.consumer_with_config(cfg);

    // manual offset management is for clients which may not process
    // one record to one offset commit
    while let Some(Ok(record)) = stream.next().await {
        println!("{}", record.get_value().as_utf8_lossy_string());

        if some_condition {
            break;
        }
        stream.offset_commit();
    }

    // synchronously flush for shutdown (or none if intentionally ending processing)
    stream.offset_flush().await?;

    Ok(())
}
```


### CLI

Some cli options would need to be added non-default topic settings for consumers, both for at the time of topic create, as well as to update offset management settings after creation (it would really stink to force a topic to be deleted to change the offset management settings).

No change to topic user create interface.

The consume command needs to accept a consumer option. The consumer is used by
the offset management to store and retrieve the offset. This maps to the `.offset_consumer(...)` builder call that
provides a consumer id.

Consumers are automatically created if they don't exist via `consume` CLI:

```
$ fluvio consume -h
  -c, --consumer <NAME>
        Creates or uses a consumer to ...
```

Create consumers:

```
$ fluvio topic create my-topic1 --partition 2
$ fluvio consume my-topic1 --consumer c1 --partition 0
$ fluvio consume my-topic1 --consumer c1 --partition 1

$ fluvio topic create my-topic2 --partition 3
$ fluvio consume my-topic2 --consumer c2 -all

$ fluvio topic create my-topic3
$ fluvio consume my-topic3 -c c3                  <--- creates a consumer at partition 0
```

List consumers:

```
$ fluvio consumer list
Consumer   Topic       Partition  Offset    TTL
c1         my-topic1       0      1342      23 hr
c1         my-topic1       1       544      12 hr
c2         my-topic2       0         2      20 min
c2         my-topic2       1         1      13 min
c2         my-topic2       2         0      10 min
c3         my-topic3       0         0      23 sec
```

Delete a consumer:

With partition:

```
$ fluvio consumer delete c1 --partition 0
consumer "c1" on partition "0" deleted
```

Without partition:
```
$ fluvio consumer delete c1
consumer "c1" on partition "0" deleted
consumer "c1" on partition "1" deleted
```


`fluvio topic describe TOPIC` should be modified to output consumer settings

```
$ fluvio topic describe my-topic1
   Name:my-topic1
   Type:computed
   Partition Count:2
   Replication Factor:1
   Ignore Rack Assignment:false
   Status:provisioned
   Reason:
   Consumers:
      - c1/0
      - c1/1
```

Deleting a `topic` will remove all consumers linked to the topic:

```
$ fluvio topic delete my-topic1
topic "my-topic1" deleted
```

```
$ fluvio consumer list
Consumer   Topic       Partition  Offset   Retention
c2         my-topic2       0         2      20 min
c2         my-topic2       1         1      13 min
c2         my-topic2       2         0      10 min
c3         my-topic3       0         0      23 sec

```
## Notes
If an internal topic is used as storage for offsets, the topics should not show up by default unless a flag requesting
the information is provided by the user.

```
$ fluvio topic list
vs
$ fluvio topic list --all
```

## References

### SSDK

SSDK has a more generalized offset management for managing its offset for state consumption. Implementation is available from that codebase for reference, but the generalization is recommended to be simplified for an in-fluvio implementation.

### Kafka
Kafka manages offsets via explicit api, or implicit with respect to consumer groups. As a consumer group updates membership and load balancing, a consumer may receive offset assignments according to the offset management of the group. This occurs implicitly unless the manual commit API is used.

https://docs.confluent.io/platform/current/clients/consumer.html

The manual commit api is documented here:
https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html

There is discussion of an async and sync commits of offsets back to the cluster. A recommendation of using async during regular consumption, and sync offset commit on consumer shutdown is mentioned.  This is because async kafka commit does not wait for confirmation and may not be guaranteed ("The main reason is that the consumer does not retry the (async) request if the commit fails."). Because kafka maintains offsets in the context of consumer groups, offset commits are intertwined with assessing membership - tracing of a given consumer is making progress.

Automatic offset consumption is also problematic as noted in the api docs:

```
Note: Using automatic offset commits can also give you "at-least-once" delivery, but the requirement is that you must consume all data returned from each call to poll(Duration) before any subsequent calls, or before closing the consumer. If you fail to do either of these, it is possible for the committed offset to get ahead of the consumed position, which results in missing records. The advantage of using manual offset control is that you have direct control over when a record is considered "consumed."
```

For a simpler fluvio interface, it is recommended to keep a simpler offset interface vs specific consumer ids, and deferring consumer group balancing (and offset management) as a separate feature.

In particular for a fluvio connector consume of an offset record would likely only after some external sink has returned with confirmation that data was committed. For a file system a confirmation might be a flush completion, or a database, a successful insert or upsert, S3 an object commit, etc. Only after the confirmation would a consumer want to commit an offset.

