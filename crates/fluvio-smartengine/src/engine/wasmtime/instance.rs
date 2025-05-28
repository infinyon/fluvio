use std::any::Any;
use std::sync::{Arc, Mutex};
use std::fmt::{self, Debug};

use tracing::debug;
use anyhow::{Error, Result};
use wasmtime::{Memory, Module, Caller, Extern, Instance, Func, AsContextMut, AsContext};

use fluvio_protocol::{Encoder, Decoder, Version};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput, SmartModuleInitInput,
};

use crate::engine::config::Lookback;
use crate::metrics::SmartModuleChainMetrics;

use super::error::EngineError;
use super::init::SmartModuleInit;
use super::look_back::SmartModuleLookBack;
use super::{WasmSlice, memory};
use super::state::WasmState;

pub(crate) struct SmartModuleInstance {
    ctx: SmartModuleInstanceContext,
    init: Option<SmartModuleInit>,
    look_back: Option<SmartModuleLookBack>,
    transform: Box<dyn DowncastableTransform>,
    version: Version,
}

impl SmartModuleInstance {
    #[cfg(test)]
    #[allow(clippy::borrowed_box)]
    pub(crate) fn transform(&self) -> &Box<dyn DowncastableTransform> {
        &self.transform
    }

    #[cfg(test)]
    pub(crate) fn get_init(&self) -> &Option<SmartModuleInit> {
        &self.init
    }

    pub(crate) fn new(
        ctx: SmartModuleInstanceContext,
        init: Option<SmartModuleInit>,
        look_back: Option<SmartModuleLookBack>,
        transform: Box<dyn DowncastableTransform>,
        version: Version,
    ) -> Self {
        Self {
            ctx,
            init,
            look_back,
            transform,
            version,
        }
    }

    pub(crate) fn process(
        &mut self,
        input: SmartModuleInput,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        // pre metrics
        let raw_len = input.raw_bytes().len();
        self.ctx.metrics().add_bytes_in(raw_len as u64);
        self.ctx.metrics().add_invocation_count(1);
        let start_time = self.ctx.metrics_time_start();

        let out = self.transform.process(input, &mut self.ctx, store);

        // post metrics
        self.ctx.metrics_time_elapsed(start_time, store);
        if let Ok(ref output) = out {
            // let num_recs = output.successes.len() as u64;
            // self.ctx.metrics().add_records_out(num_recs);

            if let Some(_err) = output.error.as_ref() {
                self.ctx.metrics().add_records_err(1);
            }
        }
        out
    }

    // TODO: Move this to SPU

    pub(crate) fn call_init(&mut self, store: &mut impl AsContextMut) -> Result<(), Error> {
        if let Some(init) = &mut self.init {
            let input = SmartModuleInitInput {
                params: self.ctx.params.clone(),
            };
            init.initialize(input, &mut self.ctx, store)
        } else {
            Ok(())
        }
    }

    pub(crate) fn call_look_back(
        &mut self,
        input: SmartModuleInput,
        store: &mut WasmState,
    ) -> Result<()> {
        if let Some(ref mut lookback) = self.look_back {
            lookback.call(input, &mut self.ctx, store)
        } else {
            Ok(())
        }
    }

    pub(crate) fn lookback(&self) -> Option<Lookback> {
        self.look_back.as_ref()?; // return None if there is no function
        self.ctx.lookback
    }

    #[allow(dead_code)]
    pub(crate) fn metrics(&self) -> Arc<SmartModuleChainMetrics> {
        self.ctx.metrics()
    }

    /// Retrieves SmartModule Version
    pub fn version(&self) -> Version {
        self.version
    }
}

pub(crate) struct SmartModuleInstanceContext {
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
    version: Version,
    lookback: Option<Lookback>,
    metrics: Arc<SmartModuleChainMetrics>,
}

impl Debug for SmartModuleInstanceContext {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleInstanceBase")
    }
}

impl SmartModuleInstanceContext {
    /// instantiate new module instance that contain context
    #[tracing::instrument(skip(state, module, params))]
    pub(crate) fn instantiate(
        state: &mut WasmState,
        module: Module,
        params: SmartModuleExtraParams,
        version: Version,
        lookback: Option<Lookback>,
        names: &[String], // smartmodule names
    ) -> Result<Self, EngineError> {
        debug!("creating WasmModuleInstance");
        let cb = Arc::new(RecordsCallBack::new());
        let records_cb = cb.clone();
        let copy_records_fn =
            move |mut caller: Caller<'_, <WasmState as AsContext>::Data>, ptr: i32, len: i32| {
                debug!(len, "callback from wasm filter");
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => anyhow::bail!("failed to find host memory"),
                };

                let records = RecordsMemory { ptr, len, memory };
                cb.set(records);
                Ok(())
            };

        debug!("instantiating WASMtime");
        let instance = state
            .instantiate(&module, copy_records_fn)
            .map_err(|e| match e.downcast::<EngineError>() {
                Ok(e) => e,
                Err(e) => EngineError::Instantiate(e),
            })?;
        let metrics = Arc::new(SmartModuleChainMetrics::new(names));
        Ok(Self {
            instance,
            records_cb,
            params,
            version,
            lookback,
            metrics,
        })
    }

    /// get wasm function from instance
    pub(crate) fn get_wasm_func(&self, store: &mut impl AsContextMut, name: &str) -> Option<Func> {
        self.instance.get_func(store, name)
    }

    pub(crate) fn write_input<E: Encoder>(
        &mut self,
        input: &E,
        store: &mut impl AsContextMut,
    ) -> Result<WasmSlice> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, self.version)?;
        debug!(
            len = input_data.len(),
            version = self.version,
            "input encoded"
        );
        let array_ptr = memory::copy_memory_to_instance(store, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32, self.version as u32))
    }

    pub(crate) fn read_output<D: Decoder + Default>(&mut self, store: impl AsContext) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from(store).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), self.version)?;
        Ok(output)
    }

    pub(crate) fn metrics(&self) -> Arc<SmartModuleChainMetrics> {
        self.metrics.clone()
    }

    /// convenience function for use with metrics_time_elapsed
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn metrics_time_start(&self) -> std::time::Instant {
        std::time::Instant::now()
    }

    // fluvio-smartengine is not compiled for wasm32, but add this
    // warning to avoid confusion should that change
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn metrics_time_start(&self) -> std::time::Instant {
        compile_error!("metrics_time_start should not be compiled for wasm32");
        unreachable!()
    }

    /// record time elapsed
    /// start_time: should come from metrics_time_start
    pub(crate) fn metrics_time_elapsed(
        &self,
        start_time: std::time::Instant,
        store: &mut WasmState,
    ) {
        let elapsed = start_time.elapsed();
        let fuel = store.get_used_fuel();
        self.metrics.add_fuel_used(fuel, elapsed);
    }
}

pub(crate) trait SmartModuleTransform: Send + Sync {
    /// transform records
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput>;

    /// return name of transform, this is used for identifying transform and debugging
    #[allow(dead_code)]
    fn name(&self) -> &str;
}

// In order turn to any, need following magic trick
pub(crate) trait DowncastableTransform: SmartModuleTransform + Any {
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn Any;
}

impl<T: SmartModuleTransform + Any> DowncastableTransform for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone)]
pub struct RecordsMemory {
    ptr: i32,
    len: i32,
    memory: Memory,
}

impl RecordsMemory {
    fn copy_memory_from(&self, store: impl AsContext) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; self.len as u32 as usize];
        self.memory.read(store, self.ptr as usize, &mut bytes)?;
        Ok(bytes)
    }
}

pub struct RecordsCallBack(Mutex<Option<RecordsMemory>>);

impl RecordsCallBack {
    pub(crate) fn new() -> Self {
        Self(Mutex::new(None))
    }

    pub(crate) fn set(&self, records: RecordsMemory) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.replace(records);
    }

    pub(crate) fn clear(&self) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.take();
    }

    pub(crate) fn get(&self) -> Option<RecordsMemory> {
        let reader = self.0.lock().unwrap();
        reader.clone()
    }
}
