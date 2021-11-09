use std::convert::TryFrom;
use anyhow::Result;
use fluvio_spu_schema::server::stream_fetch::ARRAY_MAP_WASM_API;
use wasmtime::TypedFunc;

use dataplane::smartstream::{
    SmartStreamInput, SmartStreamOutput, SmartStreamInternalError, SmartStreamExtraParams,
};
use crate::smartstream::{SmartEngine, SmartStreamModule, SmartStreamContext, SmartStream};

const ARRAY_MAP_FN_NAME: &str = "array_map";
type ArrayMapFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamArrayMap {
    base: SmartStreamContext,
    array_map_fn: ArrayMapFn,
}

impl SmartStreamArrayMap {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartStreamModule,
        params: SmartStreamExtraParams,
    ) -> Result<Self> {
        let mut base = SmartStreamContext::new(engine, module, params)?;
        let map_fn: ArrayMapFn = base
            .instance
            .get_typed_func(&mut base.store, ARRAY_MAP_FN_NAME)?;

        Ok(Self {
            base,
            array_map_fn: map_fn,
        })
    }
}

impl SmartStream for SmartStreamArrayMap {
    fn process(&mut self, input: SmartStreamInput) -> Result<SmartStreamOutput> {
        let slice = self.base.write_input(&input, ARRAY_MAP_WASM_API)?;
        let map_output = self.array_map_fn.call(&mut self.base.store, slice)?;

        if map_output < 0 {
            let internal_error = SmartStreamInternalError::try_from(map_output)
                .unwrap_or(SmartStreamInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartStreamOutput = self.base.read_output(ARRAY_MAP_WASM_API)?;
        Ok(output)
    }

    fn params(&self) -> SmartStreamExtraParams {
        self.base.params.clone()
    }
}
