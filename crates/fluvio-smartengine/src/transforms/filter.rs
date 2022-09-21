use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use wasmtime::{AsContextMut, TypedFunc};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleTransformErrorStatus,
};

use crate::{
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    WasmState,
};

pub(crate) const FILTER_FN_NAME: &str = "filter";

type WasmFilterFn = TypedFunc<(i32, i32, u32), i32>;

pub(crate) struct SmartModuleFilter(WasmFilterFn);

impl Debug for SmartModuleFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FilterFn")
    }
}

impl SmartModuleFilter {
    /// Try to create filter by matching function, if function is not found, then return empty
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>> {
        match ctx.get_wasm_func(store, FILTER_FN_NAME) {
            // check type signature
            Some(func) => func
                .typed(&mut *store)
                .or_else(|_| func.typed(store))
                .map(|filter_fn| Some(Self(filter_fn))),
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleFilter {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let filter_output = self.0.call(&mut *store, slice)?;

        if filter_output < 0 {
            let internal_error = SmartModuleTransformErrorStatus::try_from(filter_output)
                .unwrap_or(SmartModuleTransformErrorStatus::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }

    fn name(&self) -> &str {
        FILTER_FN_NAME
    }
}
