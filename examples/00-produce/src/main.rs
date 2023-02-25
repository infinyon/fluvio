//! A minimal example showing how to produce messages on Fluvio
//!
//! Before running this example, make sure you have created a topic
//! named `simple` with the following command:
//!
//! ```text
//! $ fluvio topic create simple
//! ```
//!
//! Run this example using the following:
//!
//! ```text
//! $ cargo run --bin produce
//! Sent simple record: Hello, Fluvio!
//! ```
//!
//! After running this example, you can see the messages that have
//! been sent to the topic using the following command:
//!
//! ```text
//! $ fluvio consume simple -B -d
//! Hello, Fluvio!
//! ```

use fluvio::RecordKey;

#[async_std::main]
async fn main() {
    if let Err(e) = produce().await {
        println!("Produce error: {e:?}");
    }
}

async fn produce() -> anyhow::Result<()> {
    let producer = fluvio::producer("simple").await?;

    let value = "Hello, Fluvio!";
    producer.send(RecordKey::NULL, value).await?;
    producer.flush().await?;
    println!("{value}");

    Ok(())
}
