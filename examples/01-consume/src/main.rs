//! A minimal example showing how to produce messages on Fluvio
//!
//! Before running this example, make sure you have created a topic
//! named `example` with the following command:
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

#[async_std::main]
async fn main() {
    if let Err(e) = consume().await {
        println!("Consume error: {:?}", e);
    }
}

async fn consume() -> Result<(), fluvio::FluvioError> {
    use futures_lite::StreamExt;

    let consumer = fluvio::consumer("simple", 0).await?;
    let mut stream = consumer.stream(fluvio::Offset::beginning()).await?;

    while let Some(Ok(record)) = stream.next().await {
        if let Some(bytes) = record.try_into_bytes() {
            let string = String::from_utf8_lossy(&bytes);
            println!("{}", string);
        }
    }
    Ok(())
}
