#[cfg(test)]
mod test {

    use std::convert::TryFrom;

    use fluvio_smartmodule::{dataplane::smartmodule::SmartModuleInput, Record};

    use crate::engine::common::FILTER_MAP_FN_NAME;
    use crate::engine::metrics::SmartModuleChainMetrics;
    use crate::engine::{SmartEngine, SmartModuleChainBuilder};
    use crate::engine::config::SmartModuleConfig;
    use crate::engine::fixture::read_wasm_module;

    const SM_FILTER_MAP: &str = "fluvio_smartmodule_filter_map";

    #[ignore]
    #[test]
    fn test_filter_map() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_FILTER_MAP),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain")
            .inner;

        assert_eq!(
            chain.instances().first().expect("first").transform().name(),
            FILTER_MAP_FN_NAME
        );

        let metrics = SmartModuleChainMetrics::default();
        let input = vec![Record::new("10"), Record::new("11")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 1); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"5");
    }
}
