use std::convert::TryFrom;
use anyhow::Result;
use fluvio_spu_schema::server::stream_fetch::WASM_MODULE_V2_API;
use wasmtime::TypedFunc;

use dataplane::smartstream::{
    SmartStreamInput, SmartStreamOutput, SmartStreamInternalError, SmartStreamExtraParams,
};
use crate::smartstream::{SmartStreamModule, SmartEngine, SmartStreamContext, SmartStream};

const FILTER_FN_NAME: &str = "filter";
type FilterFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamFilter {
    base: SmartStreamContext,
    filter_fn: FilterFn,
}

impl SmartStreamFilter {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartStreamModule,
        params: SmartStreamExtraParams,
    ) -> Result<Self> {
        let mut base = SmartStreamContext::new(engine, module, params)?;
        let filter_fn: FilterFn = base
            .instance
            .get_typed_func(&mut base.store, FILTER_FN_NAME)?;

        Ok(Self { base, filter_fn })
    }
}

impl SmartStream for SmartStreamFilter {
    fn process(&mut self, input: SmartStreamInput) -> Result<SmartStreamOutput> {
        let slice = self.base.write_input(&input, WASM_MODULE_V2_API)?;
        let filter_output = self.filter_fn.call(&mut self.base.store, slice)?;

        if filter_output < 0 {
            let internal_error = SmartStreamInternalError::try_from(filter_output)
                .unwrap_or(SmartStreamInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartStreamOutput = self.base.read_output(WASM_MODULE_V2_API)?;
        Ok(output)
    }

    fn params(&self) -> SmartStreamExtraParams {
        self.base.params.clone()
    }
}
