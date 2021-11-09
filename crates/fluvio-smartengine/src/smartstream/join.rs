use std::convert::TryFrom;

use anyhow::Result;
use tracing::{debug, instrument};
use wasmtime::TypedFunc;

use fluvio_spu_schema::server::stream_fetch::SMART_MODULE_API;
use dataplane::smartstream::{SmartStreamInput, SmartStreamOutput, SmartStreamInternalError};
use crate::smartstream::{
    SmartEngine, SmartStreamModule, SmartStreamContext, SmartStream, SmartStreamExtraParams,
};

const JOIN_FN_NAME: &str = "join";
type JoinFn = TypedFunc<(i32, i32), i32>;

pub struct SmartStreamJoin {
    base: SmartStreamContext,
    join_fn: JoinFn,
}

impl SmartStreamJoin {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartStreamModule,
        params: SmartStreamExtraParams,
    ) -> Result<Self> {
        let mut base = SmartStreamContext::new(engine, module, params)?;
        let join_fn: JoinFn = base
            .instance
            .get_typed_func(&mut base.store, JOIN_FN_NAME)?;

        Ok(Self { base, join_fn })
    }
}

impl SmartStream for SmartStreamJoin {
    #[instrument(skip(self, input), name = "Join")]
    fn process(&mut self, input: SmartStreamInput) -> Result<SmartStreamOutput> {
        let slice = self.base.write_input(&input, SMART_MODULE_API)?;
        debug!(len = slice.1, "WASM SLICE");
        let map_output = self.join_fn.call(&mut self.base.store, slice)?;

        if map_output < 0 {
            let internal_error = SmartStreamInternalError::try_from(map_output)
                .unwrap_or(SmartStreamInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartStreamOutput = self.base.read_output(SMART_MODULE_API)?;
        Ok(output)
    }

    fn params(&self) -> SmartStreamExtraParams {
        self.base.params.clone()
    }
}
