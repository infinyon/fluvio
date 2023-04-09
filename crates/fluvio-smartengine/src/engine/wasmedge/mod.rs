mod instance;
mod transforms;
use instance::*;
mod init;
use init::*;
mod memory;
use memory::*;

use tracing::debug;
use wasmedge_sdk::error::HostFuncError;
use wasmedge_sdk::types::Val;
use wasmedge_sdk::{
    Executor, Func, Instance, Memory, Module, Store, CallingFrame, WasmValue, Caller,
    ImportObjectBuilder,
};

use crate::engine::common::WasmFn;
use crate::engine::config::*;
use crate::engine::error::EngineError;
use crate::metrics::SmartModuleChainMetrics;
use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleExtraParams,
};
use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::{Arc, Mutex};

use self::transforms::create_transform;

pub struct WasmedgeInstance {
    instance: wasmedge_sdk::Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
    version: i16,
}

pub struct WasmedgeContext {
    engine: Executor,
}

pub type WasmedgeFn = Func;

impl super::common::WasmInstance for WasmedgeInstance {
    type Context = WasmedgeContext;

    type Func = WasmedgeFn;

    fn get_fn(&self, name: &str, _ctx: &mut Self::Context) -> Result<Option<Self::Func>> {
        match self.instance.func(name) {
            // check type signature
            Some(func) => Ok(Some(func)),
            None => Ok(None),
        }
    }

    fn write_input<E: fluvio_protocol::Encoder>(
        &mut self,
        input: &E,
        ctx: &mut Self::Context,
    ) -> Result<(i32, i32, i32)> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, self.version)?;
        debug!(
            len = input_data.len(),
            version = self.version,
            "input encoded"
        );
        let array_ptr =
            memory::copy_memory_to_instance(&mut ctx.engine, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32, self.version as i32))
    }

    fn read_output<D: fluvio_protocol::Decoder + Default>(
        &mut self,
        _ctx: &mut Self::Context,
    ) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from().ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), self.version)?;
        Ok(output)
    }
}

impl WasmFn for WasmedgeFn {
    type Context = WasmedgeContext;

    fn call(&self, ptr: i32, len: i32, version: i32, ctx: &mut Self::Context) -> Result<i32> {
        let res = self.call(
            &ctx.engine,
            vec![
                Val::I32(ptr as i32).into(),
                Val::I32(len as i32).into(),
                Val::I32(version as i32).into(),
            ],
        )?;
        Ok(res[0].to_i32())
    }
}

impl WasmedgeInstance {
    /// instantiate new module instance that contain context
    pub(crate) fn instantiate(
        store: &mut Store,
        executor: &mut Executor,
        module: Module,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self, EngineError> {
        debug!("creating WasmModuleInstance");
        let cb = Arc::new(RecordsCallBack::new());
        let records_cb = cb.clone();

        // See crates/fluvio-smartmodule-derive/src/generator/transform.rs for copy_records
        let copy_records_fn = move |caller: CallingFrame,
                                    inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            assert_eq!(inputs.len(), 2);
            let ptr = inputs[0].to_i32() as u32;
            let len = inputs[1].to_i32() as u32;

            debug!(len, "callback from wasm filter");
            let caller = Caller::new(caller);
            let memory = caller.memory(0).unwrap();

            let records = RecordsMemory { ptr, len, memory };
            cb.set(records);
            Ok(vec![])
        };

        let import = ImportObjectBuilder::new()
            .with_func::<(i32, i32), ()>("copy_records", copy_records_fn)
            .map_err(|e| EngineError::Instantiate(e.into()))?
            .build("env")
            .map_err(|e| EngineError::Instantiate(e.into()))?;

        debug!("instantiating WASMtime");
        store
            .register_import_module(executor, &import)
            .map_err(|e| EngineError::Instantiate(e.into()))?;
        let instance = store
            .register_active_module(executor, &module)
            .map_err(|e| EngineError::Instantiate(e.into()))?;

        // This is a hack to avoid them being dropped
        // FIXME: manage their lifetimes
        std::mem::forget(import);
        std::mem::forget(module);

        Ok(Self {
            instance,
            records_cb,
            params,
            version,
        })
    }
}

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
        let mut ctx = WasmedgeContext { engine: executor };

        let mut instances = Vec::with_capacity(self.smart_modules.len());
        // let mut state = engine.new_state();
        for (config, bytes) in self.smart_modules {
            let module = Module::from_bytes(None, bytes)?;
            let version = config.version();
            let mut instance = WasmedgeInstance::instantiate(
                &mut store,
                &mut ctx.engine,
                module,
                config.params,
                version,
            )?;

            // let init = SmartModuleInit::try_instantiate(&ctx, &mut state)?;
            let transform = create_transform(&mut instance, &mut ctx, config.initial_data)?;
            // instance.init(&mut state)?;
            instances.push(SmartModuleInstance {
                instance,
                transform,
            });
        }

        Ok(SmartModuleChainInstance { ctx, instances })
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
    ctx: WasmedgeContext,
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
