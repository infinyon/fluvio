use fluvio_experimental::{FluvioError, Fluvio};

fn main() {
    if let Err(e) = async_std::task::block_on(run()) {
        println!("Error: {:#?}", e);
    }
}

async fn run() -> Result<(), FluvioError> {
    let mut client = Fluvio::connect().await?;
    let topics = client.list_topics().await?;
    for topic in &topics {
        println!("Topic: {:?}", topic);
    }
    Ok(())
}
