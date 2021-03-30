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
        println!("Produce error: {:?}", e);
    }
}

async fn produce_batch() -> Result<(), fluvio::FluvioError> {
    let producer = fluvio::producer("batch").await?;

    let batch: Vec<_> = (0..10)
        .map(|i| (Some(i.to_string()), format!("This is record {}", i)))
        .collect();

    producer.send_all(batch).await?;
    Ok(())
}
