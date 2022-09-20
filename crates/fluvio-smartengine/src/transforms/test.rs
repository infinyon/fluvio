use std::{
    path::{PathBuf, Path},
    convert::TryFrom,
};

use fluvio_smartmodule::{dataplane::smartmodule::SmartModuleInput, Record};

use crate::{SmartEngine, SmartModuleConfig, SmartModuleInitialData};

const SM_FILTER: &str = "fluvio_smartmodule_filter";
const SM_ARRAY_MAP: &str = "fluvio_wasm_array_map_array";
const SM_FILTER_MAP: &str = "fluvio_wasm_filter_map";
const SM_AGGEGRATE: &str = "fluvio_wasm_aggregate";
const SM_JOIN: &str = "fluvio_wasm_join";

fn read_wasm_module(module_name: &str) -> Vec<u8> {
    let spu_dir = std::env::var("CARGO_MANIFEST_DIR").expect("target");
    let wasm_path = PathBuf::from(spu_dir)
        .parent()
        .expect("parent")
        .join(format!(
            "fluvio-smartmodule/examples/target/wasm32-unknown-unknown/release/{}.wasm",
            module_name
        ));
    read_module_from_path(wasm_path)
}

fn read_module_from_path(filter_path: impl AsRef<Path>) -> Vec<u8> {
    let path = filter_path.as_ref();
    std::fs::read(path).unwrap_or_else(|_| panic!("Unable to read file {}", path.display()))
}

#[ignore]
#[test]
fn test_filter() {
    let engine = SmartEngine::new();
    let mut chain_builder = engine.builder();

    chain_builder
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_FILTER),
        )
        .expect("failed to create filter");

    assert_eq!(
        chain_builder
            .instances()
            .first()
            .expect("first")
            .transform()
            .name(),
        crate::transforms::filter::FILTER_FN_NAME
    );

    let mut chain = chain_builder.initialize().expect("failed to build chain");

    let input = vec![Record::new("hello world")];
    let output = chain
        .process(SmartModuleInput::try_from(input).expect("input"))
        .expect("process");
    assert_eq!(output.successes.len(), 0); // no records passed

    let input = vec![Record::new("apple"), Record::new("fruit")];
    let output = chain
        .process(SmartModuleInput::try_from(input).expect("input"))
        .expect("process");
    assert_eq!(output.successes.len(), 1); // one record passed
}

#[ignore]
#[test]
fn create_filter_map() {
    let engine = SmartEngine::new();
    let mut chain = engine.builder();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_FILTER_MAP),
        )
        .expect("failed to create filter");

    // generic
    assert_eq!(
        chain.instances().first().expect("first").transform().name(),
        crate::transforms::filter_map::FILTER_MAP_FN_NAME
    );
}

#[ignore]
#[test]
fn create_array_map() {
    let engine = SmartEngine::new();
    let mut chain = engine.builder();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_ARRAY_MAP),
        )
        .expect("failed to create smart module");

    assert_eq!(
        chain.instances().first().expect("first").transform().name(),
        crate::transforms::array_map::ARRAY_MAP_FN_NAME
    );
}

#[ignore]
#[test]
fn create_aggregate() {
    let engine = SmartEngine::new();
    let mut chain = engine.builder();

    chain
        .add_smart_module(
            SmartModuleConfig::builder()
                .initial_data(SmartModuleInitialData::with_aggregate(vec![]))
                .build()
                .unwrap(),
            read_wasm_module(SM_AGGEGRATE),
        )
        .expect("failed to create smartmodule");

    assert_eq!(
        chain.instances().first().expect("first").transform().name(),
        crate::transforms::aggregate::AGGREGATE_FN_NAME
    );
}

#[ignore]
#[test]
fn create_aggregate_no_initial_data() {
    // should work with no initial data
    let engine = SmartEngine::new();
    let mut chain = engine.builder();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_AGGEGRATE),
        )
        .expect("failed to create smartmodule");

    assert_eq!(
        chain.instances().first().expect("first").transform().name(),
        crate::transforms::aggregate::AGGREGATE_FN_NAME
    );
}

#[ignore]
#[test]
fn create_join() {
    let engine = SmartEngine::new();
    let mut chain = engine.builder();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(SM_JOIN),
        )
        .expect("failed to create filter");

    assert_eq!(
        chain.instances().first().expect("first").transform().name(),
        crate::transforms::join::JOIN_FN_NAME
    );
}
