use std::convert::TryFrom;
use anyhow::Result;
use wasmtime::TypedFunc;

use fluvio_spu_schema::server::stream_fetch::WASM_MODULE_V2_API;
use dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput, SmartModuleInternalError};
use crate::smartmodule::{
    SmartEngine, SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance,
    SmartModuleExtraParams,
};

const MAP_FN_NAME: &str = "map";
type MapFn = TypedFunc<(i32, i32), i32>;

pub struct SmartModuleMap {
    base: SmartModuleContext,
    map_fn: MapFn,
}

impl SmartModuleMap {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
    ) -> Result<Self> {
        let mut base = SmartModuleContext::new(engine, module, params)?;
        let map_fn: MapFn = base.instance.get_typed_func(&mut base.store, MAP_FN_NAME)?;

        Ok(Self { base, map_fn })
    }
}

impl SmartModuleInstance for SmartModuleMap {
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        let slice = self.base.write_input(&input, WASM_MODULE_V2_API)?;
        let map_output = self.map_fn.call(&mut self.base.store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = self.base.read_output(WASM_MODULE_V2_API)?;
        Ok(output)
    }

    fn params(&self) -> SmartModuleExtraParams {
        self.base.params.clone()
    }
}
