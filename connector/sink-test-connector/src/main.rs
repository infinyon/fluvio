mod sink;

use fluvio_connector_common::{connector, Result, consumer::ConsumerStream, Sink, secret::SecretString};
use futures::SinkExt;
use sink::TestSink;

#[connector(sink)]
async fn start(config: CustomConfig, mut stream: impl ConsumerStream) -> Result<()> {
    let sink = TestSink::new(&config)?;
    let mut sink = sink.connect(None).await?;
    while let Some(item) = stream.next().await {
        let str = String::from_utf8(item?.as_ref().to_vec())?;
        sink.send(str).await?;
    }
    Ok(())
}

#[connector(config)]
struct CustomConfig {
    api_key: SecretString,
}
