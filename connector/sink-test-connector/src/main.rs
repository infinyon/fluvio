mod sink;

use fluvio_connector_common::{connector, ConnectorConfig, Result, consumer::ConsumerStream, Sink};
use futures::SinkExt;
use sink::TestSink;

#[connector(sink)]
async fn start(config: ConnectorConfig, mut stream: impl ConsumerStream) -> Result<()> {
    let sink = TestSink::new(&config)?;
    let mut sink = sink.connect(None).await?;
    while let Some(item) = stream.next().await {
        let str = String::from_utf8(item?.as_ref().to_vec())?;
        sink.send(str).await?;
    }
    Ok(())
}
