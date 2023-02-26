mod instance;
mod transforms;
use instance::*;
mod init;
use init::*;
mod memory;
use memory::*;

use tracing::debug;
use wasmedge_sdk::{Executor, Func, Instance, Memory, Module, Store};

use crate::config::*;
use crate::error::EngineError;
use crate::metrics::SmartModuleChainMetrics;
use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput};
use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::{Arc, Mutex};

use self::transforms::create_transform;

pub struct SmartEngine();

#[allow(clippy::new_without_default)]
impl SmartEngine {
    pub fn new() -> Self {
        Self()
    }
}

impl Debug for SmartEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleEngine")
    }
}

/// Building SmartModule
#[derive(Default)]
pub struct SmartModuleChainBuilder {
    smart_modules: Vec<(SmartModuleConfig, Vec<u8>)>,
}

impl SmartModuleChainBuilder {
    /// Add SmartModule with a single transform and init
    pub fn add_smart_module(&mut self, config: SmartModuleConfig, bytes: Vec<u8>) {
        self.smart_modules.push((config, bytes))
    }

    /// stop adding smartmodule and return SmartModuleChain that can be executed
    pub fn initialize(self, _engine: &SmartEngine) -> Result<SmartModuleChainInstance> {
        let mut executor = Executor::new(None, None).expect("Failed to create WasmEdge executor");
        let mut store = Store::new().expect("Failed to create WasmEdge store");

        let mut instances = Vec::with_capacity(self.smart_modules.len());
        // let mut state = engine.new_state();
        for (config, bytes) in self.smart_modules {
            let module = Module::from_bytes(None, bytes)?;
            let version = config.version();
            let ctx = SmartModuleInstanceContext::instantiate(
                &mut store,
                &mut executor,
                module,
                version,
            )?;
            // let init = SmartModuleInit::try_instantiate(&ctx, &mut state)?;
            let transform = create_transform(&ctx, config.initial_data)?;
            let instance = SmartModuleInstance::new(ctx, transform);
            // instance.init(&mut state)?;
            instances.push(instance);
        }

        Ok(SmartModuleChainInstance {
            engine: executor,
            instances,
        })
    }
}

impl<T: Into<Vec<u8>>> From<(SmartModuleConfig, T)> for SmartModuleChainBuilder {
    fn from(pair: (SmartModuleConfig, T)) -> Self {
        let mut result = Self::default();
        result.add_smart_module(pair.0, pair.1.into());
        result
    }
}

/// SmartModule Chain Instance that can be executed
pub struct SmartModuleChainInstance {
    engine: Executor,
    instances: Vec<SmartModuleInstance>,
}

impl Debug for SmartModuleChainInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleChainInstance")
    }
}

impl SmartModuleChainInstance {
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
                let output = instance.process(next_input, &mut self.engine)?;

                if output.error.is_some() {
                    // encountered error, we stop processing and return partial output
                    return Ok(output);
                } else {
                    next_input = output.successes.try_into()?;
                    next_input.set_base_offset(base_offset);
                }
            }

            let output = last.process(next_input, &mut self.engine)?;
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
