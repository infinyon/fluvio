fn main() {
    let result = async_std::task::block_on(produce());
    if let Err(e) = result {
        println!("Error: {:?}", e);
    }
}

async fn produce() -> Result<(), fluvio::FluvioError> {
    let producer = fluvio::producer("produce").await?;
    producer.send_record("Hello, Fluvio!", 0).await?;
    Ok(())
}
