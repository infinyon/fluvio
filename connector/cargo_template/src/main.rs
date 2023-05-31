mod config;
use config::CustomConfig;

{% if connector-type == "source" %}
use fluvio::{RecordKey, TopicProducer};
use fluvio_connector_common::{
    connector,
    Result 
};

#[connector(source)]
async fn start(config: CustomConfig, producer: TopicProducer) -> Result<()> {
    println!("Starting {{project-name}} source connector with {config:?}");
    for i in 1..1000 {
        let value = format!("Hello, Fluvio - {i}");
        producer.send(RecordKey::NULL, value).await?;
        producer.flush().await?;
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
    Ok(())
}

{% elsif connector-type == "sink" %}
use fluvio_connector_common::{connector, consumer::ConsumerStream, Result};

#[connector(sink)]
async fn start(config: CustomConfig, mut stream: impl ConsumerStream) -> Result<()> {
    println!("Starting {{project-name}} sink connector with {config:?}");
    while let Some(Ok(record)) = stream.next().await {
        println!("{}",record.value().as_ut8_lossy_string());
    }
    Ok(())
}
{% endif %}
