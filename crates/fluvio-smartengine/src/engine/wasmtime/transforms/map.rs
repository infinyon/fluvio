#[cfg(test)]
mod test {

    use fluvio_protocol::record::Record;
    use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

    use crate::engine::{
        SmartEngine, SmartModuleChainBuilder, SmartModuleConfig,
        wasmtime::transforms::simple_transform::MAP_FN_NAME,
    };
    use crate::engine::fixture::read_wasm_module;
    use crate::engine::config::DEFAULT_SMARTENGINE_VERSION;

    const SM_MAP: &str = "fluvio_smartmodule_map";

    #[ignore]
    #[test]
    fn test_map() {
        let engine = SmartEngine::new();
        let mut chain_builder = SmartModuleChainBuilder::default();

        let sm = read_wasm_module(SM_MAP);
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
            MAP_FN_NAME
        );

        let input = vec![Record::new("apple"), Record::new("fruit")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 2); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"APPLE");
        assert_eq!(output.successes[1].value.as_ref(), b"FRUIT");
    }
}
