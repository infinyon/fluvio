use std::time::Duration;
use fluvio::{FluvioError, Offset};

const TOPIC: &str = "echo";

fn main() {
    async_std::task::spawn(produce());
    if let Err(e) = async_std::task::block_on(consume()) {
        println!("Error: {}", e);
    }
}

async fn produce() -> Result<(), FluvioError> {
    println!("Creating producer");
    let producer = fluvio::producer(TOPIC).await?;

    for i in 0..10 {
        println!("Sending record {}", i);
        producer
            .send_record(format!("Hello Fluvio {}!", i), 0)
            .await?;
        async_std::task::sleep(Duration::from_secs(1)).await;
    }
    producer.send_record("Done!", 0).await?;

    Ok(())
}

async fn consume() -> Result<(), FluvioError> {
    println!("Creating consumer");
    let consumer = fluvio::consumer(TOPIC, 0).await?;
    println!("Creating stream");
    let mut stream = consumer.stream(Offset::beginning()).await?;

    while let Ok(event) = stream.next().await {
        println!("Received Event");
        for batch in event.partition.records.batches {
            for record in batch.records {
                if let Some(record) = record.value.inner_value() {
                    let string = String::from_utf8(record).unwrap();
                    println!("Got record: {}", string);
                    if string == "Done!" {
                        return Ok(());
                    }
                }
            }
        }
        println!("Handled event");
    }

    Ok(())
}
