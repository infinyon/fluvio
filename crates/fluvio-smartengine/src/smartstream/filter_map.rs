use std::convert::TryFrom;
use anyhow::Result;
use wasmtime::TypedFunc;

use dataplane::smartstream::{SmartStreamInput, SmartStreamOutput, SmartStreamInternalError};
use crate::smartstream::{
    SmartEngine, SmartStreamModule, SmartStreamContext, SmartStream, SmartStreamExtraParams,
};

const FILTER_MAP_FN_NAME: &str = "filter_map";
type FilterMapFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamFilterMap {
    base: SmartStreamContext,
    filter_map_fn: FilterMapFn,
}

impl SmartStreamFilterMap {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartStreamModule,
        params: SmartStreamExtraParams,
    ) -> Result<Self> {
        let mut base = SmartStreamContext::new(engine, module, params)?;
        let filter_map_fn: FilterMapFn = base
            .instance
            .get_typed_func(&mut base.store, FILTER_MAP_FN_NAME)?;

        Ok(Self {
            base,
            filter_map_fn,
        })
    }
}

impl SmartStream for SmartStreamFilterMap {
    fn process(&mut self, input: SmartStreamInput) -> Result<SmartStreamOutput> {
        let slice = self.base.write_input(&input)?;
        let map_output = self.filter_map_fn.call(&mut self.base.store, slice)?;

        if map_output < 0 {
            let internal_error = SmartStreamInternalError::try_from(map_output)
                .unwrap_or(SmartStreamInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartStreamOutput = self.base.read_output()?;
        Ok(output)
    }

    fn params(&self) -> SmartStreamExtraParams {
        self.base.params.clone()
    }
}
