use std::convert::TryFrom;
use anyhow::Result;
use wasmtime::TypedFunc;

use dataplane::smartstream::{SmartStreamInput, SmartStreamOutput, SmartStreamInternalError};
use crate::smartstream::{
    SmartStreamEngine, SmartStreamModule, SmartStreamContext, SmartStream, SmartStreamExtraParams,
};

const MAP_FN_NAME: &str = "map";
type MapFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamMap {
    base: SmartStreamContext,
    map_fn: MapFn,
}

impl SmartStreamMap {
    pub fn new(
        engine: &SmartStreamEngine,
        module: &SmartStreamModule,
        params: SmartStreamExtraParams,
    ) -> Result<Self> {
        let mut base = SmartStreamContext::new(engine, module, params)?;
        let map_fn: MapFn = base.instance.get_typed_func(&mut base.store, MAP_FN_NAME)?;

        Ok(Self { base, map_fn })
    }
}

impl SmartStream for SmartStreamMap {
    fn process(&mut self, input: SmartStreamInput) -> Result<SmartStreamOutput> {
        let slice = self.base.write_input(&input)?;
        let map_output = self.map_fn.call(&mut self.base.store, slice)?;

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
