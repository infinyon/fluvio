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
    error::Error,
    instance::{ SmartModuleInstanceContext, SmartModuleTransform},
    SmartModuleChain,
};

const JOIN_FN_NAME: &str = "join";
type BaseJoinFn = TypedFunc<(i32, i32), i32>;
type JoinFnWithParam = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleJoinStream {
    join_fn: JoinFnKind,
}

pub enum JoinFnKind {
    Base(BaseJoinFn),
    Param(JoinFnWithParam),
}

impl Debug for JoinFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(_join_fn) => write!(f, "BaseoinFn"),
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

impl SmartModuleJoinStream {
    pub(crate) fn try_instantiate(
        base: SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<Option<Self>, Error> {
        base.get_wasm_func(chain, JOIN_FN_NAME)
            .ok_or(Error::NotNamedExport(JOIN_FN_NAME))
            .and_then(|func| {
                // check type signature

                func.typed()
                    .map(|typed_fn| JoinFnKind::Base(typed_fn))
                    .or_else(|_| func.typed().map(|typed_fn| JoinFnKind::Param(typed_fn)))
                    .map(|join_fn| Some(Self { join_fn }))
                    .map_err(|wasm_err| Error::TypeConversion(JOIN_FN_NAME, wasm_err))
            })
    }
    
}

impl SmartModuleTransform for SmartModuleJoinStream {
    #[instrument(skip(self, input,ctx,chain), name = "JoinStream")]
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input,chain)?;
        debug!(len = slice.1, "WASM SLICE");
        let map_output = self.join_fn.call(chain.as_context_mut(), slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(chain)?;
        Ok(output)
    }

}
