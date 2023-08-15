use fluvio::{FluvioConfig, Fluvio, PartitionSelectionStrategy};
use fluvio::dataplane::record::ConsumerRecord;
use fluvio_connector_package::config::ConsumerPartitionConfig;
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
    cluster_config.client_id = Some(format!("fluvio_connector_{}", &config.meta().name()));

    let fluvio = Fluvio::connect_with_config(&cluster_config).await?;
    ensure_topic_exists(config).await?;

    let mut builder = fluvio::ConsumerConfig::builder();
    if let Some(max_bytes) = config.meta().consumer().and_then(|c| c.max_bytes) {
        builder.max_bytes(max_bytes.as_u64() as i32);
    }
    if let Some(smartmodules) = smartmodule_vec_from_config(config) {
        builder.smartmodule(smartmodules);
    }
    let consumer_config = builder.build()?;
    let consumer_partition = config
        .meta()
        .consumer()
        .map(|c| c.partition.clone())
        .unwrap_or_default();
    let offset = fluvio::Offset::end();
    let topic = config.meta().topic().to_string();
    let stream = match consumer_partition {
        ConsumerPartitionConfig::One(partition) => {
            let consumer = fluvio.partition_consumer(topic, partition).await?;
            consumer
                .stream_with_config(offset, consumer_config)
                .await?
                .boxed()
        }
        ConsumerPartitionConfig::All => {
            let consumer = fluvio
                .consumer(PartitionSelectionStrategy::All(topic))
                .await?;
            consumer
                .stream_with_config(offset, consumer_config)
                .await?
                .boxed()
        }
        ConsumerPartitionConfig::Many(partitions) => {
            let partitions = partitions.into_iter().map(|p| (topic.clone(), p)).collect();
            let consumer = fluvio
                .consumer(PartitionSelectionStrategy::Multiple(partitions))
                .await?;
            consumer
                .stream_with_config(offset, consumer_config)
                .await?
                .boxed()
        }
    };

    Ok((fluvio, stream))
}
