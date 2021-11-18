use std::convert::TryFrom;
use anyhow::Result;
use fluvio_spu_schema::server::stream_fetch::WASM_MODULE_V2_API;
use wasmtime::TypedFunc;

use dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError, SmartModuleExtraParams,
};
use crate::smartmodule::{SmartModuleModule, SmartEngine, SmartModuleContext, SmartModuleInstance};

const FILTER_FN_NAME: &str = "filter";
type FilterFn = TypedFunc<(i32, i32), i32>;

pub struct SmartModuleFilter {
    base: SmartModuleContext,
    filter_fn: FilterFn,
}

impl SmartModuleFilter {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartModuleModule,
        params: SmartModuleExtraParams,
    ) -> Result<Self> {
        let mut base = SmartModuleContext::new(engine, module, params)?;
        let filter_fn: FilterFn = base
            .instance
            .get_typed_func(&mut base.store, FILTER_FN_NAME)?;

        Ok(Self { base, filter_fn })
    }
}

impl SmartModuleInstance for SmartModuleFilter {
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        let slice = self.base.write_input(&input, WASM_MODULE_V2_API)?;
        let filter_output = self.filter_fn.call(&mut self.base.store, slice)?;

        if filter_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(filter_output)
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
