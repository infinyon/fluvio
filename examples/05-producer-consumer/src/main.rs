// An example of using a topic as an event bus (pub/sub channel?). producer and consumer

// 2 separate worker threads and a scheduling thread

use std::time::Duration;
use fluvio::{Offset, FluvioError, RecordKey};
use futures::StreamExt;

const PRODUCERS: u32 = 30;

#[async_std::main]
async fn main() {
    // Create multiple producers
    for id in 0..PRODUCERS {
        async_std::task::spawn(produce_records(id));
    }

    // Synchronize
    if let Err(e) = async_std::task::block_on(sync_producers()) {
        println!("Error: {}", e);
    }
}

async fn produce_records(id: u32) -> Result<(), FluvioError> {
    println!("Producer #{}", id);

    // open a connection to the sync topic
    let sync_producer = fluvio::producer("sync").await?;
    let sync_consumer = fluvio::consumer("sync", 0).await?;
    let mut sync_stream = sync_consumer.stream(Offset::end()).await?;

    // here is where we'd generate data
    //println!("{}: sleeping", id);
    async_std::task::sleep(Duration::from_secs(id.into())).await;
    //println!("{}: done sleeping", id);

    // Signal readiness
    //println!("{}: about to send ready", id);
    sync_producer
        .send(RecordKey::NULL, format!("{id}: ready"))
        .await?;
    //println!("{}: sent ready", id);
    println!("{}: waiting for start", id);
    while let Some(Ok(record)) = sync_stream.next().await {
        let _key = record
            .key()
            .map(|key| String::from_utf8_lossy(key).to_string());
        let value = String::from_utf8_lossy(record.value()).to_string();

        if value.eq("start") {
            break;
        }
    }
    //println!("{}: got a start", id);

    let producer = fluvio::producer("echo").await?;
    for i in 0..10u8 {
        producer
            .send(format!("{id}"), format!("{id}: Hello, Fluvio {}!", i))
            .await?;
        async_std::task::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn sync_producers() -> Result<(), FluvioError> {
    //println!("main: opening consumer to echo");
    let consumer = fluvio::consumer("echo", 0).await?;
    let mut stream = consumer.stream(Offset::beginning()).await?;

    //println!("main: opening consumer to sync");
    let sync_consumer = fluvio::consumer("sync", 0).await?;
    let mut sync_stream = sync_consumer.stream(Offset::end()).await?;
    let producer = fluvio::producer("sync").await?;

    // Wait for everyone to get ready

    let mut num_producers = PRODUCERS;
    println!("main: waiting on {num_producers} to get ready");
    while let Some(Ok(record)) = sync_stream.next().await {
        let _key = record
            .key()
            .map(|key| String::from_utf8_lossy(key).to_string());
        let value = String::from_utf8_lossy(record.value()).to_string();
        //println!("DEBUG: {value}");

        if value.contains("ready") {
            num_producers -= 1;
            println!("main: now waiting on {num_producers} to get ready");
        }

        if num_producers == 0 {
            println!("main: all ready, send start");
            producer.send(RecordKey::NULL, "start").await?;
            break;
        }
    }

    while let Some(Ok(record)) = stream.next().await {
        let key = record
            .key()
            .map(|key| String::from_utf8_lossy(key).to_string());
        let value = String::from_utf8_lossy(record.value()).to_string();
        println!("Got record: key={:?}, value={}", key, value);

        producer.send(RecordKey::NULL, format!("{value}")).await?;
    }
    Ok(())
}
