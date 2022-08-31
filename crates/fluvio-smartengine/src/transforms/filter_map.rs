use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInternalError,
};
use wasmtime::{AsContextMut, Trap, TypedFunc};

use crate::{
    WasmSlice,
    error::Error,
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    SmartModuleChain,
};

const FILTER_MAP_FN_NAME: &str = "filter_map";
type BaseFilterMapFn = TypedFunc<(i32, i32), i32>;
type FilterMapFnWithParam = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleFilterMap {
    filter_map_fn: FilterMapFnKind,
}

enum FilterMapFnKind {
    Base(BaseFilterMapFn),
    Param(FilterMapFnWithParam),
}

impl Debug for FilterMapFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(_) => write!(f, "BaseFilterMapFn"),
            Self::Param(_) => write!(f, "FilterMapFnWithParam"),
        }
    }
}

impl FilterMapFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Base(filter_fn) => filter_fn.call(store, (slice.0, slice.1)),
            Self::Param(filter_fn) => filter_fn.call(store, slice),
        }
    }
}

impl SmartModuleFilterMap {
    pub fn try_instantiate(
        base: SmartModuleInstanceContext,
        chain: &SmartModuleChain,
    ) -> Result<Option<Self>, Error> {
        base.get_wasm_func(chain, FILTER_MAP_FN_NAME)
            .ok_or(Error::NotNamedExport(FILTER_MAP_FN_NAME))
            .and_then(|func| {
                // check type signature

                func.typed()
                    .map(|typed_fn| FilterMapFnKind::Base(typed_fn))
                    .or_else(|_| {
                        func.typed()
                            .map(|typed_fn| FilterMapFnKind::Param(typed_fn))
                    })
                    .map(|filter_map_fn| Some(Self { filter_map_fn }))
                    .map_err(|wasm_err| Error::TypeConversion(FILTER_MAP_FN_NAME, wasm_err))
            })
    }

    /*
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
    */
}

impl SmartModuleTransform for SmartModuleFilterMap {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, chain)?;
        let map_output = self.filter_map_fn.call(chain.as_context_mut(), slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.base.read_output(chain)?;
        Ok(output)
    }
}
