use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::Result;
use wasmtime::{AsContextMut, TypedFunc};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleTransformErrorStatus,
};
use crate::{
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    state::WasmState,
};

const MAP_FN_NAME: &str = "map";

type WasmMapFn = TypedFunc<(i32, i32, u32), i32>;

pub struct SmartModuleMap(WasmMapFn);

impl Debug for SmartModuleMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MapFnWithParam")
    }
}

impl SmartModuleMap {
    #[tracing::instrument(skip(ctx, store))]
    pub(crate) fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>> {
        match ctx.get_wasm_func(store, MAP_FN_NAME) {
            Some(func) => {
                // check type signature
                func.typed(&mut *store)
                    .or_else(|_| func.typed(&mut *store))
                    .map(|map_fn| Some(Self(map_fn)))
            }
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleMap {
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
        MAP_FN_NAME
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
    use crate::fixture::read_wasm_module;

    const SM_MAP: &str = "fluvio_smartmodule_map";

    #[ignore]
    #[test]
    fn test_map() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_MAP),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        assert_eq!(
            chain.instances().first().expect("first").transform().name(),
            super::MAP_FN_NAME
        );

        let metrics = SmartModuleChainMetrics::default();
        let input = vec![Record::new("apple"), Record::new("fruit")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"APPLE");
        assert_eq!(output.successes[1].value.as_ref(), b"FRUIT");
    }
}
