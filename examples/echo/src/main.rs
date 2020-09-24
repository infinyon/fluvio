//! A simple echo application with Fluvio, to demonstrate producing and consuming messages
//!
//! Fluvio is a streaming platform with a simple API for quick development.
//! This example demonstrates how to perform two key streaming operations:
//! producing and consuming messages.
//!
//! # Installation
//!
//! Before running this example, you need to make sure you have Fluvio installed.
//! If you haven't yet, visit our [installation page], then come back here.
//!
//! Once you have the Fluvio CLI and you have a Fluvio cluster available
//! (either through minikube or Fluvio Cloud), you're ready to get started.
//!
//! [installation page]: https://nightly.fluvio.io/docs/kubernetes/install/
//!
//! # Getting Started
//!
//! When writing a streaming application, you first "produce" messages by
//! sending them to a Fluvio cluster, then you "consume" those messages
//! somewhere else by reading them back from Fluvio. In this example, we'll
//! be producing and consuming messages from the same program, but this is
//! a simple example. In a real-world application, the producer and consumer
//! may be different programs on different machines.
//!
//! Messages must be sent to a specific [Topic], which is a sort of category
//! for your events. For our echo example, we'll create a topic called "echo".
//!
//! To create your topic, run this on the command line:
//!
//! ```text
//! $ fluvio topic create echo
//! ```
//!
//! To confirm that your topic was created, we can ask Fluvio to list it back:
//!
//! ```text
//! $ fluvio topic list
//! NAME   TYPE      PARTITIONS  REPLICAS  IGNORE-RACK  STATUS                   REASON
//! echo   computed      1          1                   resolution::provisioned
//! ```
//!
//! Now, we can run the example
//!
//! ```text
//! $ cargo run --bin echo
//!    Compiling echo v0.1.0 (.../fluvio/examples/echo)
//!     Finished dev [unoptimized + debuginfo] target(s) in 6.22s
//!      Running `target/debug/echo`
//! Sending record 0
//! Got record: Hello Fluvio 0!
//! Sending record 1
//! Got record: Hello Fluvio 1!
//! Sending record 2
//! Got record: Hello Fluvio 2!
//! Sending record 3
//! Got record: Hello Fluvio 3!
//! Sending record 4
//! Got record: Hello Fluvio 4!
//! Sending record 5
//! Got record: Hello Fluvio 5!
//! Sending record 6
//! Got record: Hello Fluvio 6!
//! Sending record 7
//! Got record: Hello Fluvio 7!
//! Sending record 8
//! Got record: Hello Fluvio 8!
//! Sending record 9
//! Got record: Hello Fluvio 9!
//! Got record: Done!
//! ```
//!
//! If you want to double check that all of the messages made it into
//! the topic, you can manually consume them using the Fluvio CLI
//!
//! ```text
//! $ fluvio consume echo -B -d
//! Hello Fluvio 0!
//! Hello Fluvio 1!
//! Hello Fluvio 2!
//! Hello Fluvio 3!
//! Hello Fluvio 4!
//! Hello Fluvio 5!
//! Hello Fluvio 6!
//! Hello Fluvio 7!
//! Hello Fluvio 8!
//! Hello Fluvio 9!
//! Done!
//! ```

use std::time::Duration;
use fluvio::{FluvioError, Offset};

const TOPIC: &str = "echo";

fn main() {
    async_std::task::spawn(produce());
    if let Err(e) = async_std::task::block_on(consume()) {
        println!("Error: {}", e);
    }
}

async fn produce() -> Result<(), FluvioError> {
    let producer = fluvio::producer(TOPIC).await?;

    for i in 0..10 {
        println!("Sending record {}", i);
        producer
            .send_record(format!("Hello Fluvio {}!", i), 0)
            .await?;
        async_std::task::sleep(Duration::from_secs(1)).await;
    }
    producer.send_record("Done!", 0).await?;

    Ok(())
}

async fn consume() -> Result<(), FluvioError> {
    let consumer = fluvio::consumer(TOPIC, 0).await?;
    let mut stream = consumer.stream(Offset::beginning()).await?;

    while let Ok(event) = stream.next().await {
        for batch in event.partition.records.batches {
            for record in batch.records {
                if let Some(record) = record.value.inner_value() {
                    let string = String::from_utf8(record).unwrap();
                    println!("Got record: {}", string);
                    if string == "Done!" {
                        return Ok(());
                    }
                }
            }
        }
    }

    Ok(())
}
