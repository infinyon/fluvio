use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleTransformErrorStatus,
};
use wasmtime::{TypedFunc, AsContextMut};
use anyhow::Result;

use crate::{
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    state::WasmState,
};

type WasmFn = TypedFunc<(i32, i32, u32), i32>;

#[derive(Debug)]
pub enum SimpleTansformKind {
    Filter,
    Map,
    FilterMap,
    ArrayMap,
}

impl SimpleTansformKind {
    fn name(&self) -> &str {
        match self {
            Self::Filter => "filter",
            Self::Map => "map",
            Self::FilterMap => "filter_map",
            Self::ArrayMap => "array_map",
        }
    }
}

pub struct SimpleTansform {
    f: WasmFn,
    kind: SimpleTansformKind,
}

impl std::fmt::Debug for SimpleTansform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            SimpleTansformKind::Filter => write!(f, "FilterFn"),
            SimpleTansformKind::Map => write!(f, "MapFnWithParam"),
            SimpleTansformKind::FilterMap => write!(f, "FilterMapFnWithParam"),
            SimpleTansformKind::ArrayMap => write!(f, "ArrayMapFnWithParam"),
        }
    }
}

impl SimpleTansform {
    #[tracing::instrument(skip(ctx, store))]
    pub(crate) fn try_instantiate(
        kind: SimpleTansformKind,
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>> {
        match ctx.get_wasm_func(store, kind.name()) {
            Some(func) => {
                // check type signature
                func.typed(&mut *store)
                    .or_else(|_| func.typed(&mut *store))
                    .map(|f| Some(Self { f, kind }))
            }
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SimpleTansform {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let map_output = self.f.call(&mut *store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleTransformErrorStatus::try_from(map_output)
                .unwrap_or(SmartModuleTransformErrorStatus::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }

    fn name(&self) -> &str {
        self.kind.name()
    }
}
