use fluvio::consumer::{ConsumerConfigExtBuilder, OffsetManagementStrategy};
use fluvio::{Fluvio, FluvioConfig, Offset};
use fluvio::dataplane::record::ConsumerRecord;
use fluvio_connector_package::config::{ConsumerPartitionConfig, OffsetConfig, OffsetStrategyConfig};
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
    if let Some(consumer_id) = config.meta().consumer().and_then(|c| c.id.as_ref()) {
        builder.offset_consumer(consumer_id);
    }
    if let Some(consumer_offset) = config.meta().consumer().and_then(|c| c.offset.as_ref()) {
        let offset_strategy = match consumer_offset.strategy {
            OffsetStrategyConfig::None => OffsetManagementStrategy::None,
            OffsetStrategyConfig::Manual => OffsetManagementStrategy::Manual,
            OffsetStrategyConfig::Auto => OffsetManagementStrategy::Auto,
        };
        builder.offset_strategy(offset_strategy);
        if let Some(flush) = consumer_offset.flush_period {
            builder.offset_flush(flush);
        }
        if let Some(start) = &consumer_offset.start {
            let offsset_start = match start {
                OffsetConfig::Absolute(abs) => Offset::absolute(*abs)?,
                OffsetConfig::Beginning => Offset::beginning(),
                OffsetConfig::FromBeginning(index) => Offset::from_beginning(*index),
                OffsetConfig::End => Offset::end(),
                OffsetConfig::FromEnd(index) => Offset::from_end(*index),
            };
            builder.offset_start(offsset_start);
        }
    }

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
