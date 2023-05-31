#[cfg(test)]
mod test {

    use std::convert::TryFrom;

    use fluvio_smartmodule::{dataplane::smartmodule::SmartModuleInput, Record};

    use crate::SmartModuleInitialData;
    use crate::engine::common::AGGREGATE_FN_NAME;
    use crate::engine::metrics::SmartModuleChainMetrics;
    use crate::engine::{SmartEngine, SmartModuleChainBuilder};
    use crate::engine::config::SmartModuleConfig;
    use crate::engine::fixture::read_wasm_module;

    const SM_AGGEGRATE: &str = "fluvio_smartmodule_aggregate";

    type AggregateTransform = crate::engine::common::AggregateTransform<crate::engine::WasmFnImp>;

    #[ignore]
    #[test]
    fn test_aggregate_ok() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_AGGEGRATE),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain")
            .inner;

        assert_eq!(
            chain.instances().first().expect("first").transform().name(),
            AGGREGATE_FN_NAME
        );

        let metrics = SmartModuleChainMetrics::default();

        let input = vec![Record::new("a")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 1);
        assert_eq!(output.successes[0].value.as_ref(), b"a");

        let aggregate = chain
            .instances()
            .first()
            .expect("first")
            .transform()
            .as_any()
            .downcast_ref::<AggregateTransform>()
            .expect("aggregate");

        assert_eq!(aggregate.accumulator(), b"a");

        // new record should accumulate
        let input = vec![Record::new("b")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 1); // generate 3 records
        assert_eq!(output.successes[0].value.to_string(), "ab");

        let aggregate = chain
            .instances()
            .first()
            .expect("first")
            .transform()
            .as_any()
            .downcast_ref::<AggregateTransform>()
            .expect("aggregate");

        assert_eq!(aggregate.accumulator(), b"ab");

        // sending empty records should not clear accumulator
        let input = vec![];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 0);

        let aggregate = chain
            .instances()
            .first()
            .expect("first")
            .transform()
            .as_any()
            .downcast_ref::<AggregateTransform>()
            .expect("aggregate");

        assert_eq!(aggregate.accumulator(), b"ab");

        let input = vec![Record::new("c")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 1); // generate 3 records
        assert_eq!(output.successes[0].value.as_ref(), b"abc");
    }

    #[ignore]
    #[test]
    fn test_aggregate_with_initial() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .initial_data(SmartModuleInitialData::with_aggregate(
                    "a".to_string().as_bytes().to_vec(),
                ))
                .build()
                .unwrap(),
            read_wasm_module(SM_AGGEGRATE),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        let metrics = SmartModuleChainMetrics::default();
        // new record should accumulate
        let input = vec![Record::new("b")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 1); // generate 3 records
        assert_eq!(output.successes[0].value.as_ref(), b"ab");
    }
}
