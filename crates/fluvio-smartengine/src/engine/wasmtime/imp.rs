use anyhow::Result;
use fluvio_protocol::{Encoder, Decoder};
use tracing::debug;

use std::sync::Arc;

use fluvio_smartmodule::dataplane::smartmodule::SmartModuleExtraParams;
use wasmtime::{Instance, Store};

use crate::engine::{
    wasmtime::instance::RecordsCallBack,
    common::{WasmFn, WasmInstance},
};

pub struct WasmTimeInstance {
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
    version: i16,
}
pub struct WasmTimeContext {
    store: Store<()>,
}

pub type WasmTimeFn = wasmtime::TypedFunc<(i32, i32, i32), i32>;

impl WasmInstance for WasmTimeInstance {
    type Context = WasmTimeContext;

    type Func = WasmTimeFn;

    fn get_fn(&self, name: &str, ctx: &mut Self::Context) -> Result<Option<Self::Func>> {
        match self.instance.get_func(&mut ctx.store, name) {
            Some(func) => {
                // check type signature
                func.typed(&mut ctx.store)
                    .or_else(|_| func.typed(&ctx.store))
                    .map(|f| Some(f))
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
            &mut ctx.store,
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
            .and_then(|m| m.copy_memory_from(&ctx.store).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), self.version)?;
        Ok(output)
    }

    fn params(&self) -> SmartModuleExtraParams {
        self.params.clone()
    }
}

impl WasmFn for WasmTimeFn {
    type Context = WasmTimeContext;

    fn call(&self, ptr: i32, len: i32, version: i32, ctx: &mut Self::Context) -> Result<i32> {
        WasmTimeFn::call(self, &mut ctx.store, (ptr, len, version))
    }
}
