use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use wasmtime::{AsContextMut, Trap, TypedFunc};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError,
};
use crate::{
    WasmSlice,
    error::Error,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    SmartModuleChain,
};

const MAP_FN_NAME: &str = "map";
type BaseMapFn = TypedFunc<(i32, i32), i32>;
type MapWithParamFn = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleMap {
    map_fn: MapFnKind,
}
enum MapFnKind {
    Base(BaseMapFn),
    Param(MapWithParamFn),
}

impl Debug for MapFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(..) => write!(f, "BaseMapFn"),
            Self::Param(..) => write!(f, "MapFnWithParam"),
        }
    }
}

impl MapFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Base(map_fn) => map_fn.call(store, (slice.0, slice.1)),
            Self::Param(map_fn) => map_fn.call(store, slice),
        }
    }
}

impl SmartModuleMap {
    #[tracing::instrument(skip(base, chain))]
    pub(crate) fn try_instantiate(
        base: SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<Option<Self>, Error> {
        base.get_wasm_func(chain, MAP_FN_NAME)
            .ok_or(Error::NotNamedExport(MAP_FN_NAME))
            .and_then(|func| {
                // check type signature
                func.typed()
                    .map(|typed_fn| MapFnKind::Base(typed_fn))
                    .or_else(|_| func.typed().map(|typed_fn| MapFnKind::Param(typed_fn)))
                    .map(|map_fn| Some(Self { map_fn }))
                    .map_err(|wasm_err| Error::TypeConversion(MAP_FN_NAME, wasm_err))
            })
    }


}

impl SmartModuleTransform for SmartModuleMap {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, chain)?;
        let map_output = self.map_fn.call(chain.as_context_mut(), slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(chain)?;
        Ok(output)
    }
}
