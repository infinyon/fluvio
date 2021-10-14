use std::convert::TryFrom;
use anyhow::Result;
use wasmtime::TypedFunc;

use dataplane::smartstream::{
    SmartStreamInput, SmartStreamOutput, SmartStreamInternalError, SmartStreamExtraParams,
};
use crate::smartstream::{SmartStreamEngine, SmartStreamModule, SmartStreamContext, SmartStream};

const FLATMAP_FN_NAME: &str = "flat_map";
type FlatmapFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamFlatmap {
    base: SmartStreamContext,
    flatmap_fn: FlatmapFn,
}

impl SmartStreamFlatmap {
    pub fn new(
        engine: &SmartStreamEngine,
        module: &SmartStreamModule,
        params: SmartStreamExtraParams,
    ) -> Result<Self> {
        let mut base = SmartStreamContext::new(engine, module, params)?;
        let map_fn: FlatmapFn = base
            .instance
            .get_typed_func(&mut base.store, FLATMAP_FN_NAME)?;

        Ok(Self {
            base,
            flatmap_fn: map_fn,
        })
    }
}

impl SmartStream for SmartStreamFlatmap {
    fn process(&mut self, input: SmartStreamInput) -> Result<SmartStreamOutput> {
        let slice = self.base.write_input(&input)?;
        let map_output = self.flatmap_fn.call(&mut self.base.store, slice)?;

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
