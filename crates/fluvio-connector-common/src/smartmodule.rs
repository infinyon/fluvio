use fluvio::{FluvioConfig, SmartModuleInvocation, SmartModuleKind};
use crate::{config::ConnectorConfig, Result};
use fluvio_smartengine::transformation::TransformationConfig;

pub async fn smartmodule_chain_from_config(
    config: &ConnectorConfig,
) -> Result<Option<fluvio::SmartModuleChainBuilder>> {
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

pub fn smartmodule_vec_from_config(config: &ConnectorConfig) -> Option<Vec<SmartModuleInvocation>> {
    Some(
        config
            .transforms
            .as_ref()?
            .transforms
            .iter()
            .map(|s| SmartModuleInvocation {
                wasm: fluvio::SmartModuleInvocationWasm::Predefined(s.uses.clone()),
                kind: SmartModuleKind::Generic(Default::default()),
                params: s
                    .with
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone().into()))
                    .collect::<std::collections::BTreeMap<String, String>>()
                    .into(),
            })
            .collect(),
    )
}
