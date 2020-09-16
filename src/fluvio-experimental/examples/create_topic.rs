use fluvio_experimental::{FluvioError, FluvioClient};

fn main() {
    if let Err(e) = async_std::task::block_on(run()) {
        println!("Exited with error: {:?}", e);
    }
}

async fn run() -> Result<(), FluvioError> {
    let mut client = FluvioClient::new().await?;
    let _topic = client.create_topic("greetings").await?;
    println!("Checkpoint");
    let existing_topics = client.get_topics().await?;
    for topic in &existing_topics {
        println!("{:?}", topic);
    }
    Ok(())
}
