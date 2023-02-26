//! A minimal example showing how to produce messages on Fluvio
//!
//! This consumer will run for 3 seconds and print all of the messages
//! that it reads during that time.
//!
//! Before running this example, make sure you have created a topic
//! named `simple` with the following command:
//!
//! ```text
//! $ fluvio topic create simple
//! ```
//!
//! You will also need to send some messages to the topic. You can
//! either run the `00-produce` example to send some messages,
//! or you can use the following command:
//!
//! ```text
//! $ echo "Hello, Fluvio" | fluvio produce simple
//! ```

use std::time::Duration;
use async_std::future::timeout;

const TIMEOUT_MS: u64 = 3_000;

#[async_std::main]
async fn main() {
    // The consumer will run forever if we let it, so we set a timeout
    let result = timeout(Duration::from_millis(TIMEOUT_MS), consume()).await;

    match result {
        // Success case: timeout is up, we are done consuming
        Err(_timeout) => (),
        // We encountered an error before having a chance to time out
        Ok(Err(e)) => {
            eprintln!("Consume error: {e:?}");
        }
        // The consumer should run forever except for the timeout above
        _ => unreachable!("Consumer should last forever"),
    }
}

async fn consume() -> anyhow::Result<()> {
    use futures_lite::StreamExt;

    let consumer = fluvio::consumer("simple", 0).await?;
    let mut stream = consumer.stream(fluvio::Offset::beginning()).await?;

    while let Some(Ok(record)) = stream.next().await {
        let string = String::from_utf8_lossy(record.value());
        println!("{string}");
    }
    Ok(())
}
