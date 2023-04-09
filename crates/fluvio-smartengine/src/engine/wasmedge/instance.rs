use fluvio_protocol::{Decoder, Encoder};
use tracing::debug;
use wasmedge_sdk::error::HostFuncError;
use wasmedge_sdk::types::Val;
use wasmedge_sdk::{
    host_function, Caller, CallingFrame, Engine, Executor, Func, ImportObjectBuilder, Instance,
    Memory, Module, Store, WasmValue,
};

use super::{WasmedgeInstance, WasmedgeContext};
use super::init::SmartModuleInit;
use crate::engine::common::DowncastableTransform;
use crate::engine::error::EngineError;
use crate::metrics::SmartModuleChainMetrics;
use crate::engine::wasmedge::memory;
use crate::engine::{config::*, WasmSlice};
use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput};
use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::{Arc, Mutex};

pub(crate) struct SmartModuleInstance {
    pub instance: WasmedgeInstance,
    pub transform: Box<dyn DowncastableTransform<WasmedgeInstance>>,
    // init: Option<SmartModuleInit>,
}

impl SmartModuleInstance {
    #[cfg(test)]
    #[allow(clippy::borrowed_box)]
    pub(crate) fn transform(&self) -> &Box<dyn DowncastableTransform<WasmedgeInstance>> {
        &self.transform
    }

    #[cfg(test)]
    pub(crate) fn get_init(&self) -> &Option<SmartModuleInit> {
        &None
    }

    pub(crate) fn new(
        instance: WasmedgeInstance,
        // init: Option<SmartModuleInit>,
        transform: Box<dyn DowncastableTransform<WasmedgeInstance>>,
    ) -> Self {
        Self {
            instance,
            // init,
            transform,
        }
    }

    pub(crate) fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut WasmedgeContext,
    ) -> Result<SmartModuleOutput> {
        self.transform.process(input, &mut self.instance, ctx)
    }
}

pub struct SmartModuleInstanceContext {
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
    // params: SmartModuleExtraParams,
    version: i16,
}

impl SmartModuleInstanceContext {
    /// get wasm function from instance
    pub(crate) fn get_wasm_func(&self, name: &str) -> Option<Func> {
        self.instance.func(name)
    }

    /// instantiate new module instance that contain context
    pub(crate) fn instantiate(
        store: &mut Store,
        executor: &mut Executor,
        module: Module,
        // params: SmartModuleExtraParams,
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
            // params,
            version,
        })
    }

    pub(crate) fn write_input<E: Encoder>(
        &mut self,
        input: &E,
        engine: &impl Engine,
    ) -> Result<Vec<WasmValue>> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, self.version)?;
        debug!(
            len = input_data.len(),
            version = self.version,
            "input encoded"
        );
        let array_ptr = memory::copy_memory_to_instance(engine, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok(vec![
            Val::I32(array_ptr as i32).into(),
            Val::I32(length as i32).into(),
            Val::I32(self.version as i32).into(),
        ])
    }

    pub(crate) fn read_output<D: Decoder + Default>(&mut self) -> Result<D> {
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

// TODO: revise later to see whether Clone is necessary
#[derive(Clone)]
pub(crate) struct RecordsMemory {
    pub ptr: u32,
    pub len: u32,
    pub memory: Memory,
}

impl RecordsMemory {
    pub(crate) fn copy_memory_from(&self) -> Result<Vec<u8>> {
        let bytes = self.memory.read(self.ptr, self.len)?;
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
