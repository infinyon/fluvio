use fluvio::{FluvioConfig, SmartModuleInvocation, SmartModuleKind, SmartModuleExtraParams};

use crate::{config::ConnectorConfig, Result};

pub async fn smartmodule_chain_from_config(
    config: &ConnectorConfig,
) -> Result<Option<fluvio::SmartModuleChainBuilder>> {
    use fluvio_sc_schema::smartmodule::SmartModuleApiClient;

    let transforms = config.transforms();

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
}

pub fn smartmodule_vec_from_config(config: &ConnectorConfig) -> Option<Vec<SmartModuleInvocation>> {
    let transforms = config.transforms();

    if transforms.is_empty() {
        return Some(Vec::default());
    }

    Some(
        transforms
            .iter()
            .map(|s| SmartModuleInvocation {
                wasm: fluvio::SmartModuleInvocationWasm::Predefined(s.uses.clone()),
                kind: SmartModuleKind::Generic(Default::default()),
                params: SmartModuleExtraParams::new(
                    s.with
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone().into()))
                        .collect::<std::collections::BTreeMap<String, String>>(),
                    s.lookback.map(Into::into),
                ),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fluvio::SmartModuleInvocationWasm;
    use fluvio_connector_package::config::ConnectorConfigV1;
    use fluvio_smartengine::transformation::{TransformationStep, Lookback};

    use super::*;

    #[test]
    fn test_config_to_vec() {
        //given
        let config = ConnectorConfig::V0_1_0(ConnectorConfigV1 {
            meta: Default::default(),
            transforms: vec![TransformationStep {
                uses: "local/sm@0.0.0".to_string(),
                lookback: Some(Lookback {
                    last: 2,
                    age: Some(Duration::from_secs(10)),
                }),
                ..Default::default()
            }],
        });

        //when
        let res = smartmodule_vec_from_config(&config);

        //then
        assert!(res.is_some());
        let inv = res.unwrap().remove(0);

        assert!(
            matches!(inv.wasm, SmartModuleInvocationWasm::Predefined(s) if s.eq("local/sm@0.0.0"))
        );

        assert!(matches!(inv.kind, SmartModuleKind::Generic(_)));

        assert!(inv.params.lookback().is_some());
        assert_eq!(inv.params.lookback().unwrap().last, 2);
        assert_eq!(
            inv.params.lookback().unwrap().age,
            Some(Duration::from_secs(10))
        );
    }
}
