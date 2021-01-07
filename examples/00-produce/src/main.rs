//! A minimal example showing how to produce messages on Fluvio
//!
//! Before running this example, make sure you have created a topic
//! named `example` with the following command:
//!
//! ```text
//! $ fluvio topic create example
//! ```
//!
//! After running this example, you can see the messages that have
//! been sent to the topic using the following command:
//!
//! ```text
//! $ fluvio consume example -B -d
//! Hello, Fluvio!
//! ```

fn main() {
    let result = async_std::task::block_on(produce());
    if let Err(e) = result {
        println!("Error: {:?}", e);
    }
}

async fn produce() -> Result<(), fluvio::FluvioError> {
    let producer = fluvio::producer("example").await?;
    producer.send_record("Hello, Fluvio!", 0).await?;
    Ok(())
}
