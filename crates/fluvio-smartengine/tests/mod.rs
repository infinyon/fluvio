use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use cargo::{Config, ops};
use cargo::core::compiler::{Compilation, CompileKind, CompileMode};
use cargo::core::Workspace;
use cargo::ops::CompileOptions;
use cargo::util::interning::InternedString;

use dataplane::smartmodule::SmartModuleExtraParams;
use fluvio_smartengine::filter::SmartModuleFilter;
use fluvio_smartengine::SmartEngine;

#[ignore]
#[test]
fn test_smartmodule_instantiation() -> Result<(), Box<dyn Error>> {
    //given
    cfg_if::cfg_if! {
        if #[cfg(feature = "wasi")] {
            let target = "wasm32-wasi";
        } else {
            let target = "wasm32-unknown-unknown";
        }
    }
    println!("running smartmodule instantiation for {}", target);
    let smart_module_file_path = build_smartmodule_example("filter", target)?;
    let mut smart_module_binary = Vec::new();
    let mut file = File::open(smart_module_file_path)?;
    file.read_to_end(&mut smart_module_binary)?;
    let extra_params = SmartModuleExtraParams::default();
    let engine = SmartEngine::default();

    //when
    let module_with_engine = engine.create_module_from_binary(&smart_module_binary)?;
    let _ = SmartModuleFilter::new(&module_with_engine, extra_params, 0)?;

    //then
    Ok(())
}

fn build_smartmodule_example(name: &str, target: &str) -> Result<PathBuf, Box<dyn Error>> {
    let mut manifest_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    manifest_path.pop();
    manifest_path.push("fluvio-smartmodule/examples");
    manifest_path.push(name);
    manifest_path.push("Cargo.toml");
    let cfg = Config::default()?;
    let ws = Workspace::new(&manifest_path, &cfg)?;
    let mut options = CompileOptions::new(&cfg, CompileMode::Build)?;
    options.build_config.requested_kinds =
        CompileKind::from_requested_targets(&cfg, &[target.to_string()])?;
    options.build_config.requested_profile = InternedString::from("release");
    let Compilation { mut cdylibs, .. } = ops::compile(&ws, &options)?;
    Ok(cdylibs.remove(0).path)
}
