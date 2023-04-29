use super::instance::{WasmedgeContext, WasmedgeFn, WasmedgeInstance};

use tracing::debug;
use wasmedge_sdk::{Executor, Module, Store};
use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput};

use crate::metrics::SmartModuleChainMetrics;
use crate::engine::SmartModuleChainBuilder;
use crate::engine::common::create_transform;

type SmartModuleInit = crate::engine::common::SmartModuleInit<WasmedgeFn>;
type SmartModuleInstance = crate::engine::common::SmartModuleInstance<WasmedgeInstance, WasmedgeFn>;

#[derive(Clone)]
pub struct SmartEngineImp();

#[allow(clippy::new_without_default)]
impl SmartEngineImp {
    pub fn new() -> Self {
        Self()
    }
}

pub fn initialize_imp(
    builder: SmartModuleChainBuilder,
    _engine: &SmartEngineImp,
) -> Result<SmartModuleChainInstanceImp> {
    let executor = Executor::new(None, None).expect("Failed to create WasmEdge executor");
    let mut store = Store::new().expect("Failed to create WasmEdge store");
    let mut ctx = WasmedgeContext { engine: executor };

    let mut instances = Vec::with_capacity(builder.smart_modules.len());
    // let mut state = engine.new_state();
    for (config, bytes) in builder.smart_modules {
        let module = Module::from_bytes(None, bytes)?;
        let version = config.version();
        let mut instance = WasmedgeInstance::instantiate(
            &mut store,
            &mut ctx.engine,
            module,
            config.params,
            version,
        )?;

        let init = SmartModuleInit::try_instantiate(&mut instance, &mut ctx)?;
        let transform = create_transform(&mut instance, &mut ctx, config.initial_data)?;
        let mut instance = SmartModuleInstance {
            instance,
            transform,
            init,
        };
        instance.init(&mut ctx)?;
        instances.push(instance);
    }

    Ok(SmartModuleChainInstanceImp { ctx, instances })
}

/// SmartModule Chain Instance that can be executed
pub struct SmartModuleChainInstanceImp {
    ctx: WasmedgeContext,
    instances: Vec<SmartModuleInstance>,
}

impl SmartModuleChainInstanceImp {
    /// A single record is processed thru all smartmodules in the chain.
    /// The output of one smartmodule is the input of the next smartmodule.
    /// A single record may result in multiple records.
    /// The output of the last smartmodule is added to the output of the chain.
    pub fn process(
        &mut self,
        input: SmartModuleInput,
        metric: &SmartModuleChainMetrics,
    ) -> Result<SmartModuleOutput> {
        let raw_len = input.raw_bytes().len();
        debug!(raw_len, "sm raw input");
        metric.add_bytes_in(raw_len as u64);

        let base_offset = input.base_offset();

        if let Some((last, instances)) = self.instances.split_last_mut() {
            let mut next_input = input;

            for instance in instances {
                // pass raw inputs to transform instance
                // each raw input may result in multiple records
                // self.store.top_up_fuel();
                let output = instance.process(next_input, &mut self.ctx)?;

                if output.error.is_some() {
                    // encountered error, we stop processing and return partial output
                    return Ok(output);
                } else {
                    next_input = output.successes.try_into()?;
                    next_input.set_base_offset(base_offset);
                }
            }

            let output = last.process(next_input, &mut self.ctx)?;
            let records_out = output.successes.len();
            metric.add_records_out(records_out as u64);
            debug!(records_out, "sm records out");
            Ok(output)
        } else {
            Ok(SmartModuleOutput::new(input.try_into()?))
        }
    }

    #[cfg(test)]
    pub(crate) fn instances(&self) -> &Vec<SmartModuleInstance> {
        &self.instances
    }
}
