use std::{
    path::{PathBuf, Path},
};

use crate::{SmartEngine, SmartModuleConfig, SmartModuleInitialData};

const FLUVIO_WASM_FILTER: &str = "fluvio_wasm_filter";
const FLUVIO_WASM_ARRAY_MAP: &str = "fluvio_wasm_array_map_array";
const FLUVIO_WASM_FILTER_MAP: &str = "fluvio_wasm_filter_map";
const FLUVIO_WASM_AGGREGATE: &str = "fluvio_wasm_aggregate";
const FLUVIO_WASM_JOIN: &str = "fluvio_wasm_join";

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
fn create_filter() {
    let engine = SmartEngine::new();
    let mut chain = engine.new_chain();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(FLUVIO_WASM_FILTER),
        )
        .expect("failed to create filter");

    assert_eq!(
        chain.instances().remove(0).transform().name(),
        crate::transforms::filter::FILTER_FN_NAME
    );
}

#[ignore]
#[test]
fn create_filter_map() {
    let engine = SmartEngine::new();
    let mut chain = engine.new_chain();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(FLUVIO_WASM_FILTER_MAP),
        )
        .expect("failed to create filter");

    // generic
    assert_eq!(
        chain.instances().remove(0).transform().name(),
        crate::transforms::filter_map::FILTER_MAP_FN_NAME
    );
}

#[ignore]
#[test]
fn create_array_map() {
    let engine = SmartEngine::new();
    let mut chain = engine.new_chain();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(FLUVIO_WASM_ARRAY_MAP),
        )
        .expect("failed to create smart module");

    assert_eq!(
        chain.instances().remove(0).transform().name(),
        crate::transforms::array_map::ARRAY_MAP_FN_NAME
    );
}

#[ignore]
#[test]
fn create_aggregate() {
    let engine = SmartEngine::new();
    let mut chain = engine.new_chain();

    chain
        .add_smart_module(
            SmartModuleConfig::builder()
                .initial_data(SmartModuleInitialData::with_aggregate(vec![]))
                .build()
                .unwrap(),
            read_wasm_module(FLUVIO_WASM_AGGREGATE),
        )
        .expect("failed to create smartmodule");

    assert_eq!(
        chain.instances().remove(0).transform().name(),
        crate::transforms::aggregate::AGGREGATE_FN_NAME
    );
}

#[ignore]
#[test]
fn create_aggregate_no_initial_data() {
    // should work with no initial data
    let engine = SmartEngine::new();
    let mut chain = engine.new_chain();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(FLUVIO_WASM_AGGREGATE),
        )
        .expect("failed to create smartmodule");

    assert_eq!(
        chain.instances().remove(0).transform().name(),
        crate::transforms::aggregate::AGGREGATE_FN_NAME
    );
}

#[ignore]
#[test]
fn create_join() {
    let engine = SmartEngine::new();
    let mut chain = engine.new_chain();

    chain
        .add_smart_module(
            SmartModuleConfig::builder().build().unwrap(),
            read_wasm_module(FLUVIO_WASM_JOIN),
        )
        .expect("failed to create filter");

    assert_eq!(
        chain.instances().remove(0).transform().name(),
        crate::transforms::join::JOIN_FN_NAME
    );
}
