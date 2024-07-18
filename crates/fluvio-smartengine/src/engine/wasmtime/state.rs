use std::cmp::max;

use anyhow::Error;
use wasmtime::{
    AsContext, AsContextMut, Engine, Instance, IntoFunc, Module, Store, StoreContext,
    StoreContextMut,
};

use super::limiter::StoreResourceLimiter;

// DO NOT INCREASE THIS VALUE HIGHER THAN i64::MAX / 2.
// WASMTIME keeps fuel as i64 and has some strange behavior with `add_fuel` if trying to top fuel
// up to a values close to i64:MAX
const DEFAULT_FUEL: u64 = i64::MAX as u64 / 2;

#[derive(Debug)]
pub struct WasmState(Store<Context>);

pub struct Context {
    limiter: StoreResourceLimiter,
    wasi_ctx: wasi_common::WasiCtx,
}

impl AsContext for WasmState {
    type Data = Context;

    fn as_context(&self) -> StoreContext<'_, Self::Data> {
        self.0.as_context()
    }
}

impl AsContextMut for WasmState {
    fn as_context_mut(&mut self) -> StoreContextMut<'_, Self::Data> {
        self.0.as_context_mut()
    }
}

impl WasmState {
    // If current fuel is less than DEFAULT_FUEL, tops up fuel to DEFAULT_FUEL
    pub fn top_up_fuel(&mut self) {
        if let Ok(current_fuel) = self.0.get_fuel() {
            let _ = self.0.set_fuel(max(DEFAULT_FUEL, current_fuel));
        }
    }

    // Get amount of fuel used since last top up
    pub fn get_used_fuel(&mut self) -> u64 {
        if let Ok(current_fuel) = self.0.get_fuel() {
            max(DEFAULT_FUEL - current_fuel, 0)
        } else {
            0
        }
    }
}

impl WasmState {
    pub(crate) fn new(engine: &Engine, limiter: StoreResourceLimiter) -> Self {
        let wasi_ctx = wasi_common::sync::WasiCtxBuilder::new()
            .inherit_stderr()
            .inherit_stdout()
            .build();
        let mut s = Self(Store::new(engine, Context { limiter, wasi_ctx }));
        s.0.limiter(|inner| &mut inner.limiter);
        s.top_up_fuel();
        s
    }

    pub(crate) fn instantiate<Params, Args>(
        &mut self,
        module: &Module,
        host_fn: impl IntoFunc<<Self as AsContext>::Data, Params, Args>,
    ) -> Result<Instance, Error> {
        let mut linker = wasmtime::Linker::new(module.engine());
        wasi_common::sync::add_to_linker(&mut linker, |c: &mut Context| &mut c.wasi_ctx)?;
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

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("limiter", &self.limiter)
            .finish()
    }
}
