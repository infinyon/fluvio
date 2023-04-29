use tracing::debug;
use wasmedge_sdk::error::HostFuncError;
use wasmedge_sdk::types::Val;
use wasmedge_sdk::{
    Caller, CallingFrame, Executor, Func, ImportObjectBuilder, Module, Store, WasmValue,
};
use anyhow::Result;
use std::sync::Arc;
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;

use crate::engine::wasmedge::memory::{RecordsCallBack, RecordsMemory};
use crate::engine::common::{WasmFn, WasmInstance};
use crate::engine::error::EngineError;

pub(crate) struct WasmEdgeInstance {
    instance: wasmedge_sdk::Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
    version: i16,
}

pub(crate) struct WasmEdgeContext {
    pub engine: Executor,
}

pub type WasmEdgeFn = Func;

impl WasmInstance for WasmEdgeInstance {
    type Context = WasmEdgeContext;

    type Func = WasmEdgeFn;

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
            super::memory::copy_memory_to_instance(&ctx.engine, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr, length as i32, self.version as i32))
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

    fn params(&self) -> SmartModuleExtraParams {
        self.params.clone()
    }
}

impl WasmFn for WasmEdgeFn {
    type Context = WasmEdgeContext;

    fn call(&self, ptr: i32, len: i32, version: i32, ctx: &mut Self::Context) -> Result<i32> {
        let res = self.call(
            &ctx.engine,
            vec![
                Val::I32(ptr).into(),
                Val::I32(len).into(),
                Val::I32(version).into(),
            ],
        )?;
        Ok(res[0].to_i32())
    }
}

impl WasmEdgeInstance {
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
