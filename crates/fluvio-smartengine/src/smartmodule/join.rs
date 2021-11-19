use std::convert::TryFrom;

use anyhow::Result;
use tracing::{debug, instrument};
use wasmtime::TypedFunc;

use fluvio_spu_schema::server::stream_fetch::SMART_MODULE_API;
use dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput, SmartModuleInternalError};
use crate::smartmodule::{
    SmartEngine, SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance,
    SmartModuleExtraParams,
};

const JOIN_FN_NAME: &str = "join";
type JoinFn = TypedFunc<(i32, i32), i32>;

pub struct SmartModuleJoin {
    base: SmartModuleContext,
    join_fn: JoinFn,
}

impl SmartModuleJoin {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
    ) -> Result<Self> {
        let mut base = SmartModuleContext::new(engine, module, params)?;
        let join_fn: JoinFn = base
            .instance
            .get_typed_func(&mut base.store, JOIN_FN_NAME)?;

        Ok(Self { base, join_fn })
    }
}

impl SmartModuleInstance for SmartModuleJoin {
    #[instrument(skip(self, input), name = "Join")]
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        let slice = self.base.write_input(&input, SMART_MODULE_API)?;
        debug!(len = slice.1, "WASM SLICE");
        let map_output = self.join_fn.call(&mut self.base.store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = self.base.read_output(SMART_MODULE_API)?;
        Ok(output)
    }

    fn params(&self) -> SmartModuleExtraParams {
        self.base.params.clone()
    }
}
