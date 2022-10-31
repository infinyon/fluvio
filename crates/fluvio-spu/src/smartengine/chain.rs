use tracing::{debug, error};
use fluvio_protocol::link::ErrorCode;
use fluvio_smartengine::{
    SmartEngine, SmartModuleChainBuilder, SmartModuleChainInstance, SmartModuleConfig,
    SmartModuleInitialData,
};
use fluvio_spu_schema::server::smartmodule::{
    SmartModuleContextData, SmartModuleInvocation, SmartModuleKind,
};

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

        debug!("param: {:#?}", invocation.params);
        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .params(invocation.params)
                .version(version)
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
