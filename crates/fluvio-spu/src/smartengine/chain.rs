#[cfg(feature = "smartengine")]
use tracing::{debug, error};
use fluvio_protocol::link::ErrorCode;

#[cfg(feature = "smartengine")]
use fluvio_smartengine::{SmartModuleChainBuilder, SmartModuleConfig, SmartModuleInitialData};

#[cfg(feature = "smartengine")]
use fluvio_spu_schema::server::smartmodule::{SmartModuleContextData, SmartModuleKind};

use fluvio_spu_schema::server::smartmodule::SmartModuleInvocation;

use crate::smartengine::SmartEngine;
use crate::smartengine::SmartModuleChainInstance;

#[cfg(not(feature = "smartengine"))]
pub(crate) fn build_chain(
    _invocations: Vec<SmartModuleInvocation>,
    _version: i16,
    _engine: SmartEngine,
) -> Result<SmartModuleChainInstance, ErrorCode> {
    let smci = SmartModuleChainInstance {};
    Ok(smci)
}

#[cfg(feature = "smartengine")]
pub(crate) fn build_chain(
    invocations: Vec<SmartModuleInvocation>,
    version: i16,
    engine: SmartEngine,
) -> Result<SmartModuleChainInstance, ErrorCode> {
    let mut chain_builder = SmartModuleChainBuilder::default();
    for invocation in invocations {
        let raw = invocation
            .wasm
            .into_raw()
            .map_err(|err| ErrorCode::SmartModuleInvalid {
                error: err.to_string(),
                name: None,
            })?;

        debug!(len = raw.len(), "SmartModule with bytes");

        let initial_data = match invocation.kind {
            SmartModuleKind::Aggregate { ref accumulator } => {
                SmartModuleInitialData::with_aggregate(accumulator.clone())
            }
            SmartModuleKind::Generic(SmartModuleContextData::Aggregate { ref accumulator }) => {
                SmartModuleInitialData::with_aggregate(accumulator.clone())
            }
            _ => SmartModuleInitialData::default(),
        };

        let lookback = invocation.params.lookback().map(Into::into);

        debug!("param: {:#?}", invocation.params);
        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .params(invocation.params)
                .version(version)
                .lookback(lookback)
                .initial_data(initial_data)
                .build()
                .map_err(|err| ErrorCode::SmartModuleInvalid {
                    error: err.to_string(),
                    name: None,
                })?,
            raw,
        );
    }

    let chain = chain_builder.initialize(&engine).map_err(|err| {
        error!(
            error = err.to_string().as_str(),
            "Error Initializing SmartModule"
        );
        ErrorCode::SmartModuleChainInitError(err.to_string())
    })?;
    Ok(chain)
}
