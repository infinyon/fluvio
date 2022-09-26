use std::fmt::Debug;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput};
use tracing::{instrument};
use wasmtime::{AsContextMut, TypedFunc};

use crate::{
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    WasmState,
};

pub(crate) const JOIN_FN_NAME: &str = "join";

type WasmJoinFn = TypedFunc<(i32, i32, u32), i32>;

pub struct SmartModuleJoin(WasmJoinFn);

impl Debug for SmartModuleJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinFnWith")
    }
}

impl SmartModuleJoin {
    pub(crate) fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>> {
        match ctx.get_wasm_func(&mut *store, JOIN_FN_NAME) {
            Some(func) => {
                // check type signature

                func.typed(&mut *store)
                    .or_else(|_| func.typed(store))
                    .map(|join_fn| Some(Self(join_fn)))
            }
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleJoin {
    #[instrument(skip(self, _input, _ctx, _store), name = "Join")]
    fn process(
        &mut self,
        _input: SmartModuleInput,
        _ctx: &mut SmartModuleInstanceContext,
        _store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        /*
        let slice = ctx.write_input(&input, &mut *store)?;
        debug!(len = slice.1, "WASM SLICE");
        let map_output = self.0.call(&mut *store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleTransformErrorStatus::try_from(map_output)
                .unwrap_or(SmartModuleTransformErrorStatus::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
        */
        Err(anyhow::anyhow!("Join is disabled for now"))
    }

    fn name(&self) -> &str {
        JOIN_FN_NAME
    }
}
