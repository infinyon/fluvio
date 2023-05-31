use std::sync::{Arc, Mutex};

use anyhow::Result;
use fluvio_protocol::{Decoder, Encoder};
use tracing::debug;
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;
use wasmtime::{AsContext, Caller, Extern, Instance, Module, Memory};

use crate::engine::{
    common::{WasmFn, WasmInstance},
    error::EngineError,
};
use super::state::WasmState;

pub struct WasmtimeInstance {
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
    version: i16,
}
pub struct WasmtimeContext {
    pub state: WasmState,
}

pub type WasmtimeFn = wasmtime::TypedFunc<(i32, i32, i32), i32>;

impl WasmInstance for WasmtimeInstance {
    type Context = WasmtimeContext;

    type Func = WasmtimeFn;

    fn get_fn(&self, name: &str, ctx: &mut Self::Context) -> Result<Option<Self::Func>> {
        match self.instance.get_func(&mut ctx.state.0, name) {
            Some(func) => {
                // check type signature
                func.typed(&mut ctx.state)
                    .or_else(|_| func.typed(&ctx.state))
                    .map(Some)
            }
            None => Ok(None),
        }
    }

    fn write_input<E: Encoder>(
        &mut self,
        input: &E,
        ctx: &mut Self::Context,
    ) -> anyhow::Result<(i32, i32, i32)> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, self.version)?;
        debug!(
            len = input_data.len(),
            version = self.version,
            "input encoded"
        );
        let array_ptr = crate::engine::wasmtime::memory::copy_memory_to_instance(
            &mut ctx.state,
            &self.instance,
            &input_data,
        )?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32, self.version as i32))
    }

    fn read_output<D: Decoder + Default>(&mut self, ctx: &mut Self::Context) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from(&ctx.state).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), self.version)?;
        Ok(output)
    }

    fn params(&self) -> SmartModuleExtraParams {
        self.params.clone()
    }
}

impl WasmFn for WasmtimeFn {
    type Context = WasmtimeContext;

    fn call(&self, ptr: i32, len: i32, version: i32, ctx: &mut Self::Context) -> Result<i32> {
        WasmtimeFn::call(self, &mut ctx.state, (ptr, len, version))
    }
}

impl WasmtimeInstance {
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
