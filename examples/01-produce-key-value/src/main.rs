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
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    if let Err(e) = produce_key_value().await {
        println!("Produce error: {e:?}");
    }
}

async fn produce_key_value() -> anyhow::Result<()> {
    let producer = fluvio::producer("key-value").await?;

    let key = "Hello";
    let value = "Fluvio";

    println!("About to send");
    producer.send(key, value).await?;
    producer.flush().await?;
    println!("[{key}] {value}");
    Ok(())
}
