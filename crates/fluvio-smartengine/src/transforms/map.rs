use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use wasmtime::{AsContextMut, Trap, TypedFunc};

use dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError, SmartModuleExtraParams,
};

use crate::{
    WasmSlice, {SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance},
    error::Error,
};

const MAP_FN_NAME: &str = "map";
type OldMapFn = TypedFunc<(i32, i32), i32>;
type MapFn = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleMap {
    base: SmartModuleContext,
    map_fn: MapFnKind,
}
enum MapFnKind {
    Old(OldMapFn),
    New(MapFn),
}

impl Debug for MapFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Old(..) => write!(f, "OldMapFn"),
            Self::New(..) => write!(f, "MapFn"),
        }
    }
}

impl MapFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Old(map_fn) => map_fn.call(store, (slice.0, slice.1)),
            Self::New(map_fn) => map_fn.call(store, slice),
        }
    }
}

impl SmartModuleMap {
    pub fn new(
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self, Error> {
        let mut base = SmartModuleContext::new(module, params, version)?;
        let map_fn = if let Ok(map_fn) = base.instance.get_typed_func(&mut base.store, MAP_FN_NAME)
        {
            MapFnKind::New(map_fn)
        } else {
            let map_fn: OldMapFn = base
                .instance
                .get_typed_func(&mut base.store, MAP_FN_NAME)
                .map_err(|err| Error::NotNamedExport(MAP_FN_NAME, err))?;
            MapFnKind::Old(map_fn)
        };
        Ok(Self { base, map_fn })
    }
}

impl SmartModuleInstance for SmartModuleMap {
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        let slice = self.base.write_input(&input)?;
        let map_output = self.map_fn.call(&mut self.base.store, slice)?;

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
