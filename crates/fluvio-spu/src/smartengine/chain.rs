#[cfg(feature = "smartengine")]
use tracing::{debug, error};
use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::server::smartmodule::SmartModuleInvocation;

#[cfg(feature = "smartengine")]
use fluvio_smartengine::{EngineError, SmartModuleConfig, SmartModuleInitialData};

#[cfg(feature = "smartengine")]
use fluvio_spu_schema::server::smartmodule::{
    SmartModuleContextData, SmartModuleInvocationWasm, SmartModuleKind,
};

use crate::smartengine::SmartModuleChainBuilder;
use crate::smartengine::SmartEngine;
use crate::smartengine::SmartModuleChainInstance;

#[cfg(not(feature = "smartengine"))]
pub(crate) fn build_chain(
    mut _chain_builder: SmartModuleChainBuilder,
    _invocations: Vec<SmartModuleInvocation>,
    _version: i16,
    _engine: SmartEngine,
) -> Result<SmartModuleChainInstance, ErrorCode> {
    let smci = SmartModuleChainInstance {};
    Ok(smci)
}

#[cfg(feature = "smartengine")]
pub(crate) fn build_chain(
    mut chain_builder: SmartModuleChainBuilder,
    invocations: Vec<SmartModuleInvocation>,
    version: i16,
    engine: SmartEngine,
) -> Result<SmartModuleChainInstance, ErrorCode> {
    for invocation in invocations {
        let sm_names = match &invocation.wasm {
            SmartModuleInvocationWasm::Predefined(name) => {
                vec![name.to_owned()]
            }
            SmartModuleInvocationWasm::AdHoc(_) => {
                let name = "adhoc";
                vec![name.to_owned()]
            }
        };

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
                .smartmodule_names(sm_names)
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
        error!("Error Initializing SmartModule chain: {err:#?}");
        match err.downcast_ref() {
            Some(EngineError::StoreMemoryExceeded {
                current: _,
                requested,
                max,
            }) => ErrorCode::SmartModuleMemoryLimitExceeded {
                requested: *requested as u64,
                max: *max as u64,
            },
            _ => ErrorCode::SmartModuleChainInitError(err.to_string()),
        }
    })?;
    Ok(chain)
}
