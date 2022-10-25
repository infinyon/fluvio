use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleTransformErrorStatus,
};
use wasmtime::{AsContextMut, TypedFunc};

use crate::{
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    state::WasmState,
};

const ARRAY_MAP_FN_NAME: &str = "array_map";
type WasmArrayMapFn = TypedFunc<(i32, i32, u32), i32>;

pub(crate) struct SmartModuleArrayMap(WasmArrayMapFn);

impl Debug for SmartModuleArrayMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArrayMapFnWithParam")
    }
}

impl SmartModuleArrayMap {
    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>> {
        match ctx.get_wasm_func(&mut *store, ARRAY_MAP_FN_NAME) {
            Some(func) => {
                // check type signature

                func.typed(&mut *store)
                    .or_else(|_| func.typed(store))
                    .map(|array_map_fn| Some(Self(array_map_fn)))
            }
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleArrayMap {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        let slice = ctx.write_input(&input, &mut *store)?;
        let map_output = self.0.call(&mut *store, slice)?;

        if map_output < 0 {
            let internal_error = SmartModuleTransformErrorStatus::try_from(map_output)
                .unwrap_or(SmartModuleTransformErrorStatus::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleOutput = ctx.read_output(store)?;
        Ok(output)
    }

    fn name(&self) -> &str {
        ARRAY_MAP_FN_NAME
    }
}

#[cfg(test)]
mod test {

    use std::{convert::TryFrom};

    use fluvio_smartmodule::{
        dataplane::smartmodule::{SmartModuleInput},
        Record,
    };

    use crate::{
        SmartEngine, SmartModuleChainBuilder, SmartModuleConfig, metrics::SmartModuleChainMetrics,
    };

    const SM_ARRAY_MAP: &str = "fluvio_smartmodule_array_map_array";

    use crate::fixture::read_wasm_module;

    #[ignore]
    #[test]
    fn test_array_map() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_ARRAY_MAP),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        assert_eq!(
            chain.instances().first().expect("first").transform().name(),
            super::ARRAY_MAP_FN_NAME
        );

        let metrics = SmartModuleChainMetrics::default();

        let input = vec![Record::new("[\"Apple\",\"Banana\",\"Cranberry\"]")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 3); // generate 3 records
        assert_eq!(output.successes[0].value.as_ref(), b"\"Apple\"");
        assert_eq!(output.successes[1].value.as_ref(), b"\"Banana\"");
        assert_eq!(output.successes[2].value.as_ref(), b"\"Cranberry\"");
    }
}
