use fluvio::{FluvioConfig, Fluvio};
use fluvio::dataplane::record::ConsumerRecord;
use fluvio_sc_schema::errors::ErrorCode;
use futures::StreamExt;
use crate::{config::ConnectorConfig, Result};
use crate::ensure_topic_exists;
use crate::smartmodule::smartmodule_vec_from_config;

pub trait ConsumerStream:
    StreamExt<Item = std::result::Result<ConsumerRecord, ErrorCode>> + std::marker::Unpin
{
}

impl<T: StreamExt<Item = std::result::Result<ConsumerRecord, ErrorCode>> + std::marker::Unpin>
    ConsumerStream for T
{
}

pub async fn consumer_stream_from_config(
    config: &ConnectorConfig,
) -> Result<(Fluvio, impl ConsumerStream)> {
    let mut cluster_config = FluvioConfig::load()?;
    cluster_config.client_id = Some(format!("fluvio_connector_{}", &config.meta.name));

    let fluvio = Fluvio::connect_with_config(&cluster_config).await?;
    ensure_topic_exists(config).await?;
    let consumer = fluvio
        .partition_consumer(
            &config.meta.topic,
            config
                .meta
                .consumer
                .as_ref()
                .and_then(|c| c.partition)
                .unwrap_or_default(),
        )
        .await?;

    let mut builder = fluvio::ConsumerConfig::builder();
    if let Some(smartmodules) = smartmodule_vec_from_config(config) {
        builder.smartmodule(smartmodules);
    }
    let config = builder.build()?;
    let offset = fluvio::Offset::end();
    let stream = consumer.stream_with_config(offset, config).await?;
    Ok((fluvio, stream))
}
