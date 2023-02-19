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
//! Messages must be sent to a specific Topic, which is a sort of category
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
use fluvio::{Offset, RecordKey};
use futures::future::join;
use async_std::task::spawn;
use async_std::future::timeout;

const TOPIC: &str = "echo";
const TIMEOUT_MS: u64 = 5_000;

#[async_std::main]
async fn main() {
    let produce_handle = spawn(produce());
    let consume_handle = spawn(consume());

    let timed_result = timeout(
        Duration::from_millis(TIMEOUT_MS),
        join(produce_handle, consume_handle),
    )
    .await;

    let (produce_result, consume_result) = match timed_result {
        Ok(results) => results,
        Err(_) => {
            println!("Echo timed out after {TIMEOUT_MS}ms");
            std::process::exit(1);
        }
    };

    match (produce_result, consume_result) {
        (Err(produce_err), Err(consume_err)) => {
            println!("Echo produce error: {produce_err:?}");
            println!("Echo consume error: {consume_err:?}");
            std::process::exit(1);
        }
        (Err(produce_err), _) => {
            println!("Echo produce error: {produce_err:?}");
            std::process::exit(1);
        }
        (_, Err(consume_err)) => {
            println!("Echo consume error: {consume_err:?}");
            std::process::exit(1);
        }
        _ => (),
    }
}

/// Produces 10 "Hello, Fluvio" events, followed by a "Done!" event
async fn produce() -> anyhow::Result<()> {
    let producer = fluvio::producer(TOPIC).await?;

    for i in 0..10u32 {
        println!("Sending record {i}");
        producer
            .send(format!("Key {i}"), format!("Value {i}"))
            .await?;
    }
    producer.send(RecordKey::NULL, "Done!").await?;
    producer.flush().await?;

    Ok(())
}

/// Consumes events until a "Done!" event is read
async fn consume() -> anyhow::Result<()> {
    use futures::StreamExt;

    let consumer = fluvio::consumer(TOPIC, 0).await?;
    let mut stream = consumer.stream(Offset::beginning()).await?;

    while let Some(Ok(record)) = stream.next().await {
        let key = record
            .key()
            .map(|key| String::from_utf8_lossy(key).to_string());
        let value = String::from_utf8_lossy(record.value()).to_string();
        println!("Got record: key={key:?}, value={value}");
        if value == "Done!" {
            return Ok(());
        }
    }

    Ok(())
}
