//! A minimal example showing how to produce messages on Fluvio custom profile
//!
//! Before running this example, make sure you are logged into infinyon cloud and have created a topic
//! named `simple` with the following commands:
//!
//! ```text
//! $ fluvio cloud login
//! ```
//!
//! ```text
//! $ fluvio topic create simple
//! ```
//!
//! Run this example using the following:
//!
//! ```text
//! $ cargo run --bin produce-custom-profile
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

use fluvio::config::ConfigFile;
use fluvio::RecordKey;

#[async_std::main]
async fn main() {
    if let Err(e) = produce().await {
        println!("Produce error: {:?}", e);
    }
}

async fn produce() -> Result<(), fluvio::FluvioError> {
    let config = ConfigFile::load(None)?;
    let fluvio_config = config
        .config()
        .cluster
        .get("cloud") // set cloud profile
        .ok_or(fluvio::FluvioError::Other(
            "Error Loading cloud config file".to_string(),
        ))?;
    let fluvio_connection = fluvio::Fluvio::connect_with_config(fluvio_config);
    let producer = fluvio_connection.await?.topic_producer("simple").await?;

    let value = "Hello, Fluvio!";
    producer.send(RecordKey::NULL, value).await?;
    producer.flush().await?;
    println!("{}", value);

    Ok(())
}
