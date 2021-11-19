use std::convert::TryFrom;
use anyhow::Result;
use fluvio_spu_schema::server::stream_fetch::ARRAY_MAP_WASM_API;
use wasmtime::TypedFunc;

use dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput, SmartModuleInternalError};
use crate::smartmodule::{
    SmartEngine, SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance,
    SmartModuleExtraParams,
};

const FILTER_MAP_FN_NAME: &str = "filter_map";
type FilterMapFn = TypedFunc<(i32, i32), i32>;

pub struct SmartModuleFilterMap {
    base: SmartModuleContext,
    filter_map_fn: FilterMapFn,
}

impl SmartModuleFilterMap {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
    ) -> Result<Self> {
        let mut base = SmartModuleContext::new(engine, module, params)?;
        let filter_map_fn: FilterMapFn = base
            .instance
            .get_typed_func(&mut base.store, FILTER_MAP_FN_NAME)?;

        Ok(Self {
            base,
            filter_map_fn,
        })
    }
}

impl SmartModuleInstance for SmartModuleFilterMap {
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        let slice = self.base.write_input(&input, ARRAY_MAP_WASM_API)?;
        let map_output = self.filter_map_fn.call(&mut self.base.store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = self.base.read_output(ARRAY_MAP_WASM_API)?;
        Ok(output)
    }

    fn params(&self) -> SmartModuleExtraParams {
        self.base.params.clone()
    }
}
