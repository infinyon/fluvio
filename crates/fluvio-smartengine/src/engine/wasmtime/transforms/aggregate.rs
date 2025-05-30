use std::convert::TryFrom;
use std::fmt::Debug;

use tracing::{debug, instrument};
use anyhow::Result;
use wasmtime::{AsContextMut, TypedFunc};

use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleAggregateInput, SmartModuleAggregateOutput,
    SmartModuleTransformErrorStatus,
};
use crate::engine::SmartModuleInitialData;
use crate::engine::wasmtime::{
    instance::{SmartModuleInstanceContext, SmartModuleTransform},
    state::WasmState,
};

const AGGREGATE_FN_NAME: &str = "aggregate";

type WasmAggregateFn = TypedFunc<(i32, i32, u32), i32>;

pub(crate) struct SmartModuleAggregate {
    aggregate_fn: WasmAggregateFn,
    accumulator: Vec<u8>,
}

impl Debug for SmartModuleAggregate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AggregateFn")
    }
}

impl SmartModuleAggregate {
    #[cfg(test)]
    fn accumulator(&self) -> &[u8] {
        &self.accumulator
    }

    pub fn try_instantiate(
        ctx: &SmartModuleInstanceContext,
        initial_data: SmartModuleInitialData,
        store: &mut impl AsContextMut,
    ) -> Result<Option<Self>> {
        // get initial -data
        let accumulator = match initial_data {
            SmartModuleInitialData::Aggregate { accumulator } => accumulator,
            SmartModuleInitialData::None => {
                // if no initial data, then we initialize as default
                vec![]
            }
        };

        match ctx.get_wasm_func(&mut *store, AGGREGATE_FN_NAME) {
            Some(func) => {
                // check type signature

                func.typed(&mut *store)
                    .or_else(|_| func.typed(store))
                    .map(|aggregate_fn| {
                        Some(Self {
                            aggregate_fn,
                            accumulator,
                        })
                    })
            }
            None => Ok(None),
        }
    }
}

impl SmartModuleTransform for SmartModuleAggregate {
    #[instrument(skip(self,ctx,store),fields(offset = input.base_offset()))]
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        debug!("start aggregration");
        let input = SmartModuleAggregateInput {
            base: input,
            accumulator: self.accumulator.clone(),
        };
        let slice = ctx.write_input(&input, &mut *store)?;
        let aggregate_output = self.aggregate_fn.call(&mut *store, slice)?;

        debug!(aggregate_output);
        if aggregate_output < 0 {
            let internal_error = SmartModuleTransformErrorStatus::try_from(aggregate_output)
                .unwrap_or(SmartModuleTransformErrorStatus::UnknownError);
            return Err(internal_error.into());
        }

        let output: SmartModuleAggregateOutput = ctx.read_output(store)?;

        self.accumulator = output.accumulator;
        Ok(output.base)
    }

    fn name(&self) -> &str {
        AGGREGATE_FN_NAME
    }
}

#[cfg(test)]
mod test {

    use fluvio_protocol::record::Record;
    use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

    use crate::engine::{
        SmartEngine, SmartModuleChainBuilder, SmartModuleConfig, SmartModuleInitialData,
    };
    use crate::engine::config::DEFAULT_SMARTENGINE_VERSION;

    const SM_AGGEGRATE: &str = "fluvio_smartmodule_aggregate";

    use crate::engine::fixture::read_wasm_module;

    #[ignore]
    #[test]
    fn test_aggregate_ok() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        let sm = read_wasm_module(SM_AGGEGRATE);
        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .smartmodule_names(&[sm.0])
                .build()
                .unwrap(),
            sm.1,
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        assert_eq!(
            chain.instances().first().expect("first").transform().name(),
            super::AGGREGATE_FN_NAME
        );

        let input = vec![Record::new("a")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1);
        assert_eq!(output.successes[0].value.as_ref(), b"a");

        let aggregate = chain
            .instances()
            .first()
            .expect("first")
            .transform()
            .as_any()
            .downcast_ref::<super::SmartModuleAggregate>()
            .expect("aggregate");

        assert_eq!(aggregate.accumulator(), b"a");

        // new record should accumulate
        let input = vec![Record::new("b")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1); // generate 3 records
        assert_eq!(output.successes[0].value.to_string(), "ab");

        let aggregate = chain
            .instances()
            .first()
            .expect("first")
            .transform()
            .as_any()
            .downcast_ref::<super::SmartModuleAggregate>()
            .expect("aggregate");

        assert_eq!(aggregate.accumulator(), b"ab");

        // sending empty records should not clear accumulator
        let input: Vec<Record> = vec![];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 0);

        let aggregate = chain
            .instances()
            .first()
            .expect("first")
            .transform()
            .as_any()
            .downcast_ref::<super::SmartModuleAggregate>()
            .expect("aggregate");

        assert_eq!(aggregate.accumulator(), b"ab");

        let input = vec![Record::new("c")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1); // generate 3 records
        assert_eq!(output.successes[0].value.as_ref(), b"abc");
    }

    #[ignore]
    #[test]
    fn test_aggregate_with_initial() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        let sm = read_wasm_module(SM_AGGEGRATE);
        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .smartmodule_names(&[sm.0])
                .initial_data(SmartModuleInitialData::with_aggregate(
                    "a".to_string().as_bytes().to_vec(),
                ))
                .build()
                .unwrap(),
            sm.1,
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        // new record should accumulate
        let input = vec![Record::new("b")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1); // generate 3 records
        assert_eq!(output.successes[0].value.as_ref(), b"ab");
    }
}
