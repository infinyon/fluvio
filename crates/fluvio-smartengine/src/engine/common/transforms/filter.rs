#[cfg(test)]
mod test {

    use std::convert::TryFrom;

    use fluvio_smartmodule::{dataplane::smartmodule::SmartModuleInput, Record};

    use crate::engine::common::FILTER_FN_NAME;
    use crate::engine::metrics::SmartModuleChainMetrics;
    use crate::engine::{SmartEngine, SmartModuleChainBuilder};
    use crate::engine::config::SmartModuleConfig;
    use crate::engine::fixture::read_wasm_module;

    const SM_FILTER: &str = "fluvio_smartmodule_filter";
    const SM_FILTER_INIT: &str = "fluvio_smartmodule_filter_init";

    #[ignore]
    #[test]
    fn test_filter() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_FILTER),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain")
            .inner;

        assert_eq!(
            chain.instances().first().expect("first").transform().name(),
            FILTER_FN_NAME
        );

        let metrics = SmartModuleChainMetrics::default();
        let input = vec![Record::new("hello world")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 0); // no records passed

        let input = vec![Record::new("apple"), Record::new("fruit")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 1); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"apple");
    }

    #[ignore]
    #[test]
    fn test_filter_with_init_invalid_param() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_FILTER_INIT),
        );

        assert_eq!(
            chain_builder
                .initialize(&engine)
                .expect_err("should return param error")
                .to_string(),
            "Missing param key\n\nSmartModule Init Error: \n"
        );
    }

    #[ignore]
    #[test]
    fn test_filter_with_init_ok() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .param("key", "a")
                .build()
                .unwrap(),
            read_wasm_module(SM_FILTER_INIT),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain")
            .inner;

        let instance = chain.instances().first().expect("first");

        assert_eq!(instance.transform().name(), FILTER_FN_NAME);

        assert!(instance.get_init().is_some());

        let metrics = SmartModuleChainMetrics::default();

        let input = vec![Record::new("hello world")];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 0); // no records passed

        let input = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];

        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"apple");
        assert_eq!(output.successes[1].value.as_ref(), b"banana");

        // build 2nd chain with different parameter
        let mut chain_builder = SmartModuleChainBuilder::default();
        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .param("key", "b")
                .build()
                .unwrap(),
            read_wasm_module(SM_FILTER_INIT),
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        let input = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];
        let output = chain
            .process(SmartModuleInput::try_from(input).expect("input"), &metrics)
            .expect("process");
        assert_eq!(output.successes.len(), 1); // only banana
        assert_eq!(output.successes[0].value.as_ref(), b"banana");
    }
}
