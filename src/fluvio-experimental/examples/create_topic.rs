use fluvio_experimental::{FluvioError, Fluvio};

fn main() {
    if let Err(e) = async_std::task::block_on(run()) {
        println!("Exited with error: {:?}", e);
    }
}

async fn run() -> Result<(), FluvioError> {
    let mut client = Fluvio::connect().await?;
    let _topic = client.create_topic("greetings").await?;
    println!("Checkpoint");
    let existing_topics = client.list_topics().await?;
    for topic in &existing_topics {
        println!("{:?}", topic);
    }
    Ok(())
}
