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

const ARRAY_MAP_FN_NAME: &str = "array_map";
type BaseArrayMapFn = TypedFunc<(i32, i32), i32>;
type ArrayMapFnWithParam = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub struct SmartModuleArrayMap {
    array_map_fn: ArrayMapFnKind,
}

enum ArrayMapFnKind {
    Base(BaseArrayMapFn),
    Param(ArrayMapFnWithParam),
}

impl Debug for ArrayMapFnKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Base(..) => write!(f, "BaseArrayMapFn"),
            Self::Param(..) => write!(f, "ArrayMapFnWithParam"),
        }
    }
}

impl ArrayMapFnKind {
    fn call(&self, store: impl AsContextMut, slice: WasmSlice) -> Result<i32, Trap> {
        match self {
            Self::Base(array_map_fn) => array_map_fn.call(store, (slice.0, slice.1)),
            Self::Param(array_map_fn) => array_map_fn.call(store, slice),
        }
    }
}

impl SmartModuleArrayMap {
    pub fn try_instantiate(
        base: SmartModuleInstanceContext,
        chain: &SmartModuleChain,
    ) -> Result<Option<Self>, Error> {
        base.get_wasm_func(chain, ARRAY_MAP_FN_NAME)
            .ok_or(Error::NotNamedExport(ARRAY_MAP_FN_NAME))
            .and_then(|func| {
                // check type signature

                func.typed()
                    .map(|typed_fn| ArrayMapFnKind::Base(typed_fn))
                    .or_else(|_| func.typed().map(|typed_fn| ArrayMapFnKind::Param(typed_fn)))
                    .map(|array_map_fn| Some(Self { array_map_fn }))
                    .map_err(|wasm_err| Error::TypeConversion(ARRAY_MAP_FN_NAME, wasm_err))
            })
    }

    /*
    pub fn new(
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self, Error> {
        let mut base = SmartModuleContext::new(module, params, version)?;
        let map_fn = if let Ok(array_map_fn) = base
            .instance
            .get_typed_func(&mut base.store, ARRAY_MAP_FN_NAME)
        {
            ArrayMapFnKind::New(array_map_fn)
        } else {
            let array_map_fn = base
                .instance
                .get_typed_func(&mut base.store, ARRAY_MAP_FN_NAME)
                .map_err(|err| Error::NotNamedExport(ARRAY_MAP_FN_NAME, err))?;
            ArrayMapFnKind::Old(array_map_fn)
        };

        Ok(Self {
            base,
            array_map_fn: map_fn,
        })
    }
    */
}

impl SmartModuleTransform for SmartModuleArrayMap {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, chain)?;
        let map_output = self.array_map_fn.call(chain.as_context_mut(), slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleInternalError::try_from(map_output)
                .unwrap_or(SmartModuleInternalError::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.base.read_output(chain)?;
        Ok(output)
    }
}
