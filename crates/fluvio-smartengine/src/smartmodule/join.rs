use std::convert::TryFrom;

use anyhow::Result;
use tracing::{debug, instrument};
use wasmtime::{AsContextMut, Trap, TypedFunc};

use dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput, SmartModuleInternalError};
use crate::{
    WasmSlice,
    smartmodule::{
        SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance, SmartModuleExtraParams,
    },
};

const JOIN_FN_NAME: &str = "join";
type OldJoinFn = TypedFunc<(i32, i32), i32>;
type JoinFn = TypedFunc<(i32, i32, u32), i32>;

pub struct SmartModuleJoin {
    base: SmartModuleContext,
    join_fn: JoinFnKind,
}

pub enum JoinFnKind {
    Old(OldJoinFn),
    New(JoinFn),
}

impl JoinFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Old(join_fn) => join_fn.call(store, (slice.0, slice.1)),
            Self::New(join_fn) => join_fn.call(store, slice),
        }
    }
}

impl SmartModuleJoin {
    pub fn new(
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self> {
        let mut base = SmartModuleContext::new(module, params, version)?;
        let join_fn =
            if let Ok(join_fn) = base.instance.get_typed_func(&mut base.store, JOIN_FN_NAME) {
                JoinFnKind::New(join_fn)
            } else {
                let join_fn = base
                    .instance
                    .get_typed_func(&mut base.store, JOIN_FN_NAME)?;
                JoinFnKind::Old(join_fn)
            };
        Ok(Self { base, join_fn })
    }
}

impl SmartModuleInstance for SmartModuleJoin {
    #[instrument(skip(self, input), name = "Join")]
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        let slice = self.base.write_input(&input)?;
        debug!(len = slice.1, "WASM SLICE");
        let map_output = self.join_fn.call(&mut self.base.store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = self.base.read_output()?;
        Ok(output)
    }

    fn params(&self) -> SmartModuleExtraParams {
        self.base.params.clone()
    }
}
