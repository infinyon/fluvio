use futures_lite::StreamExt;

fn main() {
    let result = async_std::task::block_on(consume());
    if let Err(e) = result {
        println!("Error: {:?}", e);
    }
}

async fn consume() -> Result<(), fluvio::FluvioError> {
    let consumer = fluvio::consumer("greetings0", 0).await?;
    let mut stream = consumer.stream(fluvio::Offset::beginning()).await?;

    while let Some(Ok(record)) = stream.next().await {
        if let Some(bytes) = record.try_into_bytes() {
            let string = String::from_utf8_lossy(&bytes);
            println!("Got record: {}", string);
        }
    }
    Ok(())
}
