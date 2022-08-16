use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use wasmtime::{AsContextMut, Trap, TypedFunc};

use dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError, SmartModuleExtraParams,
};
use crate::{
    WasmSlice,
    {SmartModuleWithEngine, SmartModuleContext, SmartModuleInstance, error::Error},
};

const FILTER_FN_NAME: &str = "filter";
type OldFilterFn = TypedFunc<(i32, i32), i32>;
type FilterFn = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleFilter {
    base: SmartModuleContext,
    filter_fn: FilterFnKind,
}

enum FilterFnKind {
    Old(OldFilterFn),
    New(FilterFn),
}

impl Debug for FilterFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Old(_) => write!(f, "OldFilterFn"),
            Self::New(_) => write!(f, "FilterFn"),
        }
    }
}

impl FilterFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Old(filter_fn) => filter_fn.call(store, (slice.0, slice.1)),
            Self::New(filter_fn) => filter_fn.call(store, slice),
        }
    }
}

impl SmartModuleFilter {
    pub fn new(
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self, Error> {
        let mut base = SmartModuleContext::new(module, params, version)?;
        let filter_fn = if let Ok(filt_fn) = base
            .instance
            .get_typed_func(&mut base.store, FILTER_FN_NAME)
        {
            FilterFnKind::New(filt_fn)
        } else {
            let filt_fn: OldFilterFn = base
                .instance
                .get_typed_func(&mut base.store, FILTER_FN_NAME)
                .map_err(|err| Error::NotNamedExport("filter", err))?;
            FilterFnKind::Old(filt_fn)
        };

        Ok(Self { base, filter_fn })
    }
}

impl SmartModuleInstance for SmartModuleFilter {
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput> {
        let slice = self.base.write_input(&input)?;
        let filter_output = self.filter_fn.call(&mut self.base.store, slice)?;

        if filter_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(filter_output)
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
