use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput, SmartModuleInternalError,
};
use tracing::{debug, instrument};
use wasmtime::{AsContextMut, Trap, TypedFunc};

use crate::WasmSlice;

use super::{SmartModuleContext, SmartModuleWithEngine, error::Error, SmartModuleInstance};

const JOIN_FN_NAME: &str = "join";
type OldJoinFn = TypedFunc<(i32, i32), i32>;
type JoinFn = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleJoin {
    base: SmartModuleContext,
    join_fn: JoinFnKind,
}

pub enum JoinFnKind {
    Old(OldJoinFn),
    New(JoinFn),
}

impl Debug for JoinFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Old(_join_fn) => write!(f, "OldJoinFn"),
            Self::New(_join_fn) => write!(f, "JoinFn"),
        }
    }
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
    ) -> Result<Self, Error> {
        let mut base = SmartModuleContext::new(module, params, version)?;
        let join_fn =
            if let Ok(join_fn) = base.instance.get_typed_func(&mut base.store, JOIN_FN_NAME) {
                JoinFnKind::New(join_fn)
            } else {
                let join_fn = base
                    .instance
                    .get_typed_func(&mut base.store, JOIN_FN_NAME)
                    .map_err(|err| Error::NotNamedExport(JOIN_FN_NAME, err))?;
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
        self.base.get_params().clone()
    }

    fn mut_ctx(&mut self) -> &mut SmartModuleContext {
        &mut self.base
    }
}
