use std::cmp::max;

use anyhow::Error;
use wasmtime::{
    AsContext, AsContextMut, Engine, Instance, IntoFunc, Module, Store, StoreContext,
    StoreContextMut,
};

const DEFAULT_FUEL: u64 = u64::MAX;

#[cfg(not(feature = "wasi"))]
pub type WasmState = WasmStore<()>;

#[cfg(feature = "wasi")]
pub type WasmState = WasmStore<wasmtime_wasi::WasiCtx>;

#[derive(Debug)]
pub struct WasmStore<T>(Store<T>);

impl<T> AsContext for WasmStore<T> {
    type Data = T;

    fn as_context(&self) -> StoreContext<'_, Self::Data> {
        self.0.as_context()
    }
}

impl<T> AsContextMut for WasmStore<T> {
    fn as_context_mut(&mut self) -> StoreContextMut<'_, Self::Data> {
        self.0.as_context_mut()
    }
}

impl WasmState {
    // If current fuel is less than DEFAULT_FUEL, tops up fuel to DEFAULT_FUEL
    pub fn top_up_fuel(&mut self) {
        if let Ok(current_fuel) = self.0.consume_fuel(0) {
            let amount_to_add = max(DEFAULT_FUEL - current_fuel, 0);
            let _ = self.0.add_fuel(amount_to_add);
        }
    }
}

#[cfg(not(feature = "wasi"))]
impl WasmStore<()> {
    pub(crate) fn new(engine: &Engine) -> Self {
        let mut s = Self(Store::new(engine, ()));
        s.top_up_fuel();
        s
    }

    pub(crate) fn instantiate<Params, Args>(
        &mut self,
        module: &Module,
        host_fn: impl IntoFunc<<Self as AsContext>::Data, Params, Args>,
    ) -> Result<Instance, Error> {
        use wasmtime::Func;

        let func = Func::wrap(&mut *self, host_fn);
        Instance::new(self, module, &[func.into()])
    }
}

#[cfg(feature = "wasi")]
impl WasmStore<wasmtime_wasi::WasiCtx> {
    pub(crate) fn new(engine: &Engine) -> Self {
        let wasi = wasmtime_wasi::WasiCtxBuilder::new()
            .inherit_stderr()
            .inherit_stdout()
            .build();
        let mut s = Self(Store::new(engine, wasi));
        s.top_up_fuel();
        s
    }

    pub(crate) fn instantiate<Params, Args>(
        &mut self,
        module: &Module,
        host_fn: impl IntoFunc<<Self as AsContext>::Data, Params, Args>,
    ) -> Result<Instance, Error> {
        let mut linker = wasmtime::Linker::new(module.engine());
        wasmtime_wasi::add_to_linker(&mut linker, |c| c)?;
        let copy_records_fn_import = module
            .imports()
            .find(|import| import.name().eq("copy_records"))
            .ok_or_else(|| Error::msg("At least one import is required"))?;
        linker.func_wrap(
            copy_records_fn_import.module(),
            copy_records_fn_import.name(),
            host_fn,
        )?;
        linker.instantiate(self, module)
    }
}
