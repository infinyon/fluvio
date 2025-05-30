#[cfg(test)]
mod test {

    use fluvio_protocol::record::Record;
    use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

    use crate::engine::{
        SmartEngine, SmartModuleChainBuilder, SmartModuleConfig,
        wasmtime::transforms::simple_transform::FILTER_FN_NAME,
    };
    use crate::engine::config::DEFAULT_SMARTENGINE_VERSION;

    const SM_FILTER: &str = "fluvio_smartmodule_filter";
    const SM_FILTER_INIT: &str = "fluvio_smartmodule_filter_init";

    use crate::engine::fixture::read_wasm_module;

    #[ignore]
    #[test]
    fn test_filter() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        let sm = read_wasm_module(SM_FILTER);
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
            FILTER_FN_NAME
        );

        let input = vec![Record::new("hello world")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 0); // no records passed

        let input = vec![Record::new("apple"), Record::new("fruit")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"apple");
    }

    #[ignore]
    #[test]
    fn test_filter_with_init_invalid_param() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        let sm = read_wasm_module(SM_FILTER_INIT);
        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .smartmodule_names(&[sm.0])
                .build()
                .unwrap(),
            sm.1,
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

        let sm = read_wasm_module(SM_FILTER_INIT);
        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .smartmodule_names(&[sm.0])
                .param("key", "a")
                .build()
                .unwrap(),
            sm.1,
        );

        let mut chain = chain_builder
            .initialize(&engine)
            .expect("failed to build chain");

        let instance = chain.instances().first().expect("first");

        assert_eq!(instance.transform().name(), FILTER_FN_NAME);

        assert!(instance.get_init().is_some());

        let input = vec![Record::new("hello world")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 0); // no records passed

        let input = vec![
            Record::new("apple"),
            Record::new("fruit"),
            Record::new("banana"),
        ];

        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"apple");
        assert_eq!(output.successes[1].value.as_ref(), b"banana");

        // build 2nd chain with different parameter
        let mut chain_builder = SmartModuleChainBuilder::default();
        let sm = read_wasm_module(SM_FILTER_INIT);
        chain_builder.add_smart_module(
            SmartModuleConfig::builder()
                .smartmodule_names(&[sm.0])
                .param("key", "b")
                .build()
                .unwrap(),
            sm.1,
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
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1); // only banana
        assert_eq!(output.successes[0].value.as_ref(), b"banana");
    }
}
