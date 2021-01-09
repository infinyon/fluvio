//! A minimal example showing how to produce messages on Fluvio
//!
//! Before running this example, make sure you have created a topic
//! named `example` with the following command:
//!
//! ```text
//! $ fluvio topic create example
//! ```
//!
//! You will also need to send some messages to the topic. You can
//! either run the `00-produce` example to send some messages,
//! or you can use the following command:
//!
//! ```text
//! $ echo "Hello, Fluvio" | fluvio produce example
//! ```

fn main() {
    let result = async_std::task::block_on(consume());
    if let Err(e) = result {
        println!("Consume error: {:?}", e);
    }
}

async fn consume() -> Result<(), fluvio::FluvioError> {
    use futures_lite::StreamExt;

    let consumer = fluvio::consumer("simple-example", 0).await?;
    let mut stream = consumer.stream(fluvio::Offset::beginning()).await?;

    while let Some(Ok(record)) = stream.next().await {
        if let Some(bytes) = record.try_into_bytes() {
            let string = String::from_utf8_lossy(&bytes);
            println!("{}", string);
        }
    }
    Ok(())
}
