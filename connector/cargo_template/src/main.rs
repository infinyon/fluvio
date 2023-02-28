mod config;
use config::{{project-name}}Config;

{% if connector-type == "source" %}
use fluvio::{RecordKey, TopicProducer};
use fluvio_connector_common::{
    connector,
    tracing::{debug, trace},
    Result, Source,
};

#[connector(source)]
async fn start(config: {{project-name}}Config, consumer: TopicProducer) -> Result<()> {
	Ok(())
}
{% elsif connector-type == "sink" %}
use fluvio_connector_common::{connector, consumer::ConsumerStream, tracing::trace, Result, Sink};

#[connector(sink)]
async fn start(config: {{project-name}}Config, mut stream: impl ConsumerStream) -> Result<()> {
	Ok(())
}
{% endif %}
