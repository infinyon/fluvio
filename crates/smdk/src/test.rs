use std::env;
use std::{collections::BTreeMap, path::PathBuf};
use std::fmt::Debug;

use cargo_metadata::{MetadataCommand, CargoOpt};
use clap::Parser;
use anyhow::Result;

use fluvio::{FluvioError, RecordKey};
use fluvio_protocol::record::{RecordData, Record};

use fluvio_smartengine::{SmartEngine, SmartModuleConfig};

use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

use tracing::debug;

/// Test SmartModule
#[derive(Debug, Parser)]
pub struct TestOpt {
    // json value
    #[clap(long)]
    input: Option<String>,

    // arbitrary file
    #[clap(long)]
    file: Option<PathBuf>,

    // optional wasm_file path
    #[clap(long)]
    wasm_file: Option<PathBuf>,

    /// (Optional) Extra input parameters passed to the smartmodule module.
    /// They should be passed using key=value format
    /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1

    #[clap(
        short = 'e',
        long= "params",
        parse(try_from_str = parse_key_val),
        number_of_values = 1
    )]
    params: Vec<(String, String)>,
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!(format!("invalid KEY=value: no `=` found in `{}`", s)))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

impl TestOpt {
    fn wasm_file_path(&self) -> Result<PathBuf> {
        let _metadata = MetadataCommand::new()
            .manifest_path("./Cargo.toml")
            .features(CargoOpt::AllFeatures)
            .exec()?;
        let mut path = env::current_dir()?;
        path.push("target");
        path.push("wasm32-unknown-unknown");
        path.push("release-lto");
        path.push("fluvio_smartmodule_map.wasm ");
        Ok(path)
    }
    pub(crate) fn process(self) -> Result<()> {
        debug!("starting smart module test");

        // load wasm file
        let wasm_path = self.wasm_file_path()?;
        println!("loading module at: {}", wasm_path.display());
        let raw = std::fs::read(self.wasm_file_path()?)?;

        let param: BTreeMap<String, String> = self.params.into_iter().collect();

        let engine = SmartEngine::new();
        let mut chain_builder = engine.builder();
        chain_builder
            .add_smart_module(
                SmartModuleConfig::builder().params(param.into()).build()?,
                raw,
            )
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;

        println!("SmartModule created");

        let mut chain = chain_builder
            .initialize()
            .map_err(|e| FluvioError::Other(format!("SmartEngine init - {:?}", e)))?;

        // get raw json in one of other ways
        let raw_input = if let Some(input) = self.input {
            debug!(input, "input string");
            input.as_bytes().to_vec()
        } else if let Some(json_file) = &self.file {
            std::fs::read(json_file)?
        } else {
            return Err(anyhow::anyhow!("No json provided"));
        };

        debug!(len = raw_input.len(), "input data");

        let record_value: RecordData = raw_input.into();
        let entries = vec![Record::new_key_value(RecordKey::NULL, record_value)];

        let output = chain
            .process(SmartModuleInput::try_from(entries)?)
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;

        println!("{:?} records", output.successes.len());
        for output_record in output.successes {
            let output_value = output_record
                .value
                .as_str()
                .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;
            println!("{}", output_value);
        }

        Ok(())
    }
}

//  target/release/fluvio sm test --input ww --regex "[A-Z]" --wasm-file target/wasm32-unknown-unknown/release-lto/fluvio_wasm_component.wasm
