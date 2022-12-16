mod source;

use fluvio::{TopicProducer, RecordKey};
use fluvio_connector_common::{Source, connector, ConnectorConfig, Result};
use futures::StreamExt;

use crate::source::TestJsonSource;

#[connector(source)]
async fn start(config: ConnectorConfig, producer: TopicProducer) -> Result<()> {
    let source = TestJsonSource::new(&config)?;
    let mut stream = source.connect(None).await?;
    while let Some(item) = stream.next().await {
        println!("producing a value: {}", &item);
        producer.send(RecordKey::NULL, item).await?;
    }
    Ok(())
}
