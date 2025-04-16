#[cfg(test)]
mod test {

    use fluvio_protocol::record::Record;
    use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

    use crate::engine::{
        SmartEngine, SmartModuleChainBuilder, SmartModuleConfig,
        wasmtime::transforms::simple_transform::FILTER_MAP_FN_NAME,
    };
    use crate::engine::config::DEFAULT_SMARTENGINE_VERSION;

    const SM_FILTER_MAP: &str = "fluvio_smartmodule_filter_map";

    use crate::engine::fixture::read_wasm_module;

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
            .expect("failed to build chain");

        assert_eq!(
            chain.instances().first().expect("first").transform().name(),
            FILTER_MAP_FN_NAME
        );

        let input = vec![Record::new("10"), Record::new("11")];
        let output = chain
            .process(
                SmartModuleInput::try_from_records(input, DEFAULT_SMARTENGINE_VERSION)
                    .expect("input"),
            )
            .expect("process");
        assert_eq!(output.successes.len(), 1); // one record passed
        assert_eq!(output.successes[0].value.as_ref(), b"5");
    }
}
