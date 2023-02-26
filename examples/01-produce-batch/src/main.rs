//! A minimal example showing how to produce batches of records with Fluvio
//!
//! Before running this example, make sure you have created a topic
//! named `batch` with the following command:
//!
//! ```text
//! $ fluvio topic create batch
//! ```
//!
//! Run this example using the following:
//!
//! ```text
//! $ cargo run --bin produce-batch
//! ```
//!
//! After running this example, you can see the messages that have
//! been sent to the topic using the following command:
//!
//! ```text
//! $ fluvio consume batch -B -d
//! [Hello] Fluvio
//! ```

#[async_std::main]
async fn main() {
    if let Err(e) = produce_batch().await {
        println!("Produce error: {e:?}");
    }
}

async fn produce_batch() -> anyhow::Result<()> {
    let producer = fluvio::producer("batch").await?;

    for i in 0..10 {
        producer
            .send(i.to_string(), format!("This is record {i}"))
            .await?;
    }
    // Only flush after `.send`ing all records
    // Be sure not to flush after every single send
    producer.flush().await?;

    Ok(())
}
