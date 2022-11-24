use std::any::Any;
use std::sync::{Arc, Mutex};
use std::fmt::{self, Debug};

use tracing::{debug};
use anyhow::{Error, Result};
use wasmtime::{Memory, Module, Caller, Extern, Instance, Func, AsContextMut, AsContext};

use fluvio_protocol::{Encoder, Decoder};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput, SmartModuleInitInput,
};

use crate::error::EngineError;
use crate::init::SmartModuleInit;
use crate::{WasmSlice, memory};
use crate::state::WasmState;

pub(crate) struct SmartModuleInstance {
    ctx: SmartModuleInstanceContext,
    init: Option<SmartModuleInit>,
    transform: Box<dyn DowncastableTransform>,
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
        transform: Box<dyn DowncastableTransform>,
    ) -> Self {
        Self {
            ctx,
            init,
            transform,
        }
    }

    pub(crate) fn process(
        &mut self,
        input: SmartModuleInput,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        self.transform.process(input, &mut self.ctx, store)
    }

    // TODO: Move this to SPU

    pub fn init(&mut self, store: &mut impl AsContextMut) -> Result<(), Error> {
        if let Some(init) = &mut self.init {
            let input = SmartModuleInitInput {
                params: self.ctx.params.clone(),
            };
            init.initialize(input, &mut self.ctx, store)
        } else {
            Ok(())
        }
    }
}

pub(crate) struct SmartModuleInstanceContext {
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
    version: i16,
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
        version: i16,
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
            .map_err(EngineError::Instantiate)?;
        Ok(Self {
            instance,
            records_cb,
            params,
            version,
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
    fn name(&self) -> &str;
}

// In order turn to any, need following magic trick
pub(crate) trait DowncastableTransform: SmartModuleTransform + Any {
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
