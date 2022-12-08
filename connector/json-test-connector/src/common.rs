use fluvio::{Offset, FluvioConfig, metadata::topic::TopicSpec};
use fluvio_connector_package::config::ConnectorConfig;
use fluvio_smartengine::transformation::TransformationConfig;
use futures::stream::LocalBoxStream;
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub(crate) trait Source<'a, I> {
    async fn connect(self, offset: Option<Offset>) -> Result<LocalBoxStream<'a, I>>;
}

pub(crate) async fn create_producer(
    config: &ConnectorConfig,
) -> anyhow::Result<fluvio::TopicProducer> {
    let mut cluster_config = FluvioConfig::load()?;
    cluster_config.client_id = Some(format!("fluvio_connector_{}", &config.name));

    let fluvio = fluvio::Fluvio::connect_with_config(&cluster_config).await?;
    ensure_topic_exists(config).await?;
    let mut config_builder = fluvio::TopicProducerConfigBuilder::default();

    if let Some(producer_params) = &config.producer {
        // Linger
        if let Some(linger) = producer_params.linger {
            config_builder = config_builder.linger(linger)
        };

        // Compression
        if let Some(compression) = producer_params.compression {
            config_builder = config_builder.compression(compression)
        };

        // Batch size
        if let Some(batch_size) = producer_params.batch_size {
            config_builder = config_builder.batch_size(batch_size.as_u64() as usize)
        };
    };

    let producer_config = config_builder.build()?;
    let producer = fluvio
        .topic_producer_with_config(&config.topic, producer_config)
        .await?;

    if let Some(chain) = create_smart_module_chain(config).await? {
        Ok(producer.with_chain(chain)?)
    } else {
        Ok(producer)
    }
}
pub(crate) async fn ensure_topic_exists(config: &ConnectorConfig) -> anyhow::Result<()> {
    let admin = fluvio::FluvioAdmin::connect().await?;
    let topics = admin.list::<TopicSpec, String>(vec![]).await?;
    let topic_exists = topics.iter().any(|t| t.name == config.topic.clone());
    if !topic_exists {
        let _ = admin
            .create(
                config.topic.clone(),
                false,
                TopicSpec::new_computed(1, 1, Some(false)),
            )
            .await;
    }
    Ok(())
}
pub async fn create_smart_module_chain(
    config: &ConnectorConfig,
) -> anyhow::Result<Option<fluvio::SmartModuleChainBuilder>> {
    use fluvio_sc_schema::smartmodule::SmartModuleApiClient;

    if let Some(TransformationConfig { transforms }) = &config.transforms {
        if transforms.is_empty() {
            return Ok(None);
        }
        let api_client =
            SmartModuleApiClient::connect_with_config(FluvioConfig::load()?.try_into()?).await?;
        let mut builder = fluvio::SmartModuleChainBuilder::default();
        for step in transforms {
            let wasm = api_client
                .get(step.uses.clone())
                .await?
                .ok_or_else(|| anyhow::anyhow!("smartmodule {} not found", step.uses))?
                .wasm
                .as_raw_wasm()?;

            let config = fluvio::SmartModuleConfig::from(step.clone());
            builder.add_smart_module(config, wasm);
        }
        Ok(Some(builder))
    } else {
        Ok(None)
    }
}
