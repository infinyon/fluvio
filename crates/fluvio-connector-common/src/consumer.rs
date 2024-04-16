use fluvio::consumer::ConsumerConfigExtBuilder;
use fluvio::{FluvioConfig, Fluvio};
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

    let consumer_partition = config
        .meta()
        .consumer()
        .map(|c| c.partition.clone())
        .unwrap_or_default();
    let mut builder = ConsumerConfigExtBuilder::default();
    builder.topic(config.meta().topic());
    builder.offset_start(fluvio::Offset::end());
    match consumer_partition {
        ConsumerPartitionConfig::One(partition) => {
            builder.partition(partition);
        }
        ConsumerPartitionConfig::All => (),
        ConsumerPartitionConfig::Many(partitions) => {
            for partition in partitions {
                builder.partition(partition);
            }
        }
    };
    if let Some(max_bytes) = config.meta().consumer().and_then(|c| c.max_bytes) {
        builder.max_bytes(max_bytes.as_u64() as i32);
    }
    if let Some(smartmodules) = smartmodule_vec_from_config(config) {
        builder.smartmodule(smartmodules);
    }

    let stream = fluvio.consumer_with_config(builder.build()?).await?.boxed();

    Ok((fluvio, stream))
}
