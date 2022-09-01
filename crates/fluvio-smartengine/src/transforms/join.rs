use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError,
};
use tracing::{debug, instrument};
use wasmtime::{AsContextMut, Trap, TypedFunc};

use crate::{
    WasmSlice,
    error::EngineError,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    WasmState,
};

const JOIN_FN_NAME: &str = "join";
type BaseJoinFn = TypedFunc<(i32, i32), i32>;
type JoinFnWithParam = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleJoin {
    join_fn: JoinFnKind,
}

pub enum JoinFnKind {
    Base(BaseJoinFn),
    Param(JoinFnWithParam),
}

impl Debug for JoinFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(_join_fn) => write!(f, "BaseJoinFn"),
            Self::Param(_join_fn) => write!(f, "JoinFnWithParam"),
        }
    }
}

impl JoinFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Base(join_fn) => join_fn.call(store, (slice.0, slice.1)),
            Self::Param(join_fn) => join_fn.call(store, slice),
        }
    }
}

impl SmartModuleJoin {
    pub(crate) fn try_instantiate(
        base: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>, EngineError> {
        base.get_wasm_func(&mut *store, JOIN_FN_NAME)
            .ok_or(EngineError::NotNamedExport(JOIN_FN_NAME))
            .and_then(|func| {
                // check type signature

                func.typed(&mut *store)
                    .map(|typed_fn| JoinFnKind::Base(typed_fn))
                    .or_else(|_| {
                        func.typed(store)
                            .map(|typed_fn| JoinFnKind::Param(typed_fn))
                    })
                    .map(|join_fn| Some(Self { join_fn }))
                    .map_err(|wasm_err| EngineError::TypeConversion(JOIN_FN_NAME, wasm_err))
            })
    }
}

impl SmartModuleTransform for SmartModuleJoin {
    #[instrument(skip(self, input, ctx, store), name = "Join")]
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        debug!(len = slice.1, "WASM SLICE");
        let map_output = self.join_fn.call(&mut *store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }
}
