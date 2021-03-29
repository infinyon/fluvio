//! A minimal example showing how to produce key-value records with Fluvio
//!
//! Before running this example, make sure you have created a topic
//! named `key-value` with the following command:
//!
//! ```text
//! $ fluvio topic create key-value
//! ```
//!
//! Run this example using the following:
//!
//! ```text
//! $ cargo run --bin produce-key-value
//! ```
//!
//! After running this example, you can see the messages that have
//! been sent to the topic using the following command:
//!
//! ```text
//! $ fluvio consume key-value -B -d
//! [Hello] Fluvio
//! ```

#[async_std::main]
async fn main() {
    if let Err(e) = produce_key_value().await {
        println!("Produce error: {:?}", e);
    }
}

async fn produce_key_value() -> Result<(), fluvio::FluvioError> {
    let producer = fluvio::producer("key-value").await?;

    let key = "Hello";
    let value = "Fluvio";

    producer.send(key, value).await?;
    println!("[{}] {}", key, value);
    Ok(())
}
