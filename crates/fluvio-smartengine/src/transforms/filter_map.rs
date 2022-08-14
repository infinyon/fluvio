use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use wasmtime::{AsContextMut, Trap, TypedFunc};

use dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleExtraParams, SmartModuleInternalError,
};

use crate::{
    WasmSlice,
    {SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance},
    error::Error,
};

const FILTER_MAP_FN_NAME: &str = "filter_map";
type OldFilterMapFn = TypedFunc<(i32, i32), i32>;
type FilterMapFn = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleFilterMap {
    base: SmartModuleContext,
    filter_map_fn: FilterMapFnKind,
}

enum FilterMapFnKind {
    Old(OldFilterMapFn),
    New(FilterMapFn),
}

impl Debug for FilterMapFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Old(_) => write!(f, "OldFilterMapFn"),
            Self::New(_) => write!(f, "FilterMapFn"),
        }
    }
}

impl FilterMapFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Old(filter_fn) => filter_fn.call(store, (slice.0, slice.1)),
            Self::New(filter_fn) => filter_fn.call(store, slice),
        }
    }
}

impl SmartModuleFilterMap {
    pub fn new(
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self, Error> {
        let mut base = SmartModuleContext::new(module, params, version)?;
        let filter_map_fn = if let Ok(fmap_fn) = base
            .instance
            .get_typed_func(&mut base.store, FILTER_MAP_FN_NAME)
        {
            FilterMapFnKind::New(fmap_fn)
        } else {
            let fmap_fn: OldFilterMapFn = base
                .instance
                .get_typed_func(&mut base.store, FILTER_MAP_FN_NAME)
                .map_err(|err| Error::NotNamedExport(FILTER_MAP_FN_NAME, err))?;
            FilterMapFnKind::Old(fmap_fn)
        };

        Ok(Self {
            base,
            filter_map_fn,
        })
    }
}

impl SmartModuleInstance for SmartModuleFilterMap {
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        let slice = self.base.write_input(&input)?;
        let map_output = self.filter_map_fn.call(&mut self.base.store, slice)?;

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
