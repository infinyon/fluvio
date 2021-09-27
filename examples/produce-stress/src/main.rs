//! This example shows how producing many records quickly will cause them to be batched

use fluvio::RecordKey;

#[async_std::main]
async fn main() {
    if let Err(e) = produce().await {
        println!("Produce error: {:?}", e);
    }
}

async fn produce() -> Result<(), fluvio::FluvioError> {
    let producer = fluvio::producer("stress").await?;

    for i in 0..100_000 {
        println!("Caller sending {}", i);
        producer.send(RecordKey::NULL, i.to_string()).await?;
    }

    producer.flush().await?;
    Ok(())
}
