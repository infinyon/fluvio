use fluvio_experimental::{FluvioError, FluvioClient};

fn main() {
    if let Err(e) = async_std::task::block_on(run()) {
        println!("Error: {:#?}", e);
    }
}

async fn run() -> Result<(), FluvioError> {
    let mut client = FluvioClient::new().await?;
    let topics = client.get_topics().await?;
    for topic in &topics {
        println!("Topic: {:?}", topic);
    }
    Ok(())
}
