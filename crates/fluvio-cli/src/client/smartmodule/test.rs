use std::{collections::BTreeMap, path::PathBuf};
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;

use fluvio::dataplane::smartmodule::{
    LegacySmartModulePayload, SmartModuleWasmCompressed, SmartModuleKind,
};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::target::ClusterTarget;
use fluvio_smartengine::SmartEngine;

use fluvio::{
    Fluvio, FluvioError,
    dataplane::{
        smartmodule::SmartModuleInput,
        record::{RecordData, Record},
    },
    RecordKey,
};
use tracing::debug;

use crate::{Result, error::CliError, client::cmd::ClientCmd};

/// Test SmartModule
/// This is a unstable feature and is not yet ready for public use.
/// This requires init function to be implemented.
#[derive(Debug, Parser)]
pub struct TestSmartModuleOpt {
    // json value
    #[clap(long)]
    input: Option<String>,

    // arbitrary file
    #[clap(long)]
    file: Option<PathBuf>,

    #[clap(long)]
    wasm_file: PathBuf,

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
    //#[clap(long)]
    //regex: String,
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s.find('=').ok_or_else(|| {
        CliError::InvalidArg(format!("invalid KEY=value: no `=` found in `{}`", s))
    })?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[async_trait]
impl ClientCmd for TestSmartModuleOpt {
    async fn process<O: Terminal + Send + Sync + Debug>(
        self,
        _out: Arc<O>,
        _target: ClusterTarget,
    ) -> Result<()> {
        debug!("starting smart module test");
        let param: BTreeMap<String, String> = self.params.into_iter().collect();

        // load wasm file
        let raw = std::fs::read(self.wasm_file)?;

        let payload = LegacySmartModulePayload {
            wasm: SmartModuleWasmCompressed::Raw(raw),
            kind: SmartModuleKind::Filter,
            params: param.into(),
        };

        debug!("loading module");
        let engine = SmartEngine::default();
        let mut smartmodule = engine
            .create_module_from_payload(payload, None)
            .map_err(|e| FluvioError::Other(format!("SmartEngine - {:?}", e)))?;

        debug!("SmartModule created");

        smartmodule
            .invoke_constructor()
            .map_err(|e| FluvioError::Other(format!("SmartEngine constructor - {:?}", e)))?;

        // get raw json in one of other ways
        let raw_input = if let Some(input) = self.input {
            debug!(input, "input string");
            input.as_bytes().to_vec()
        } else if let Some(json_file) = &self.file {
            std::fs::read(json_file)?
        } else {
            return Err(CliError::Other("No json provided".to_string()));
        };

        debug!(len = raw_input.len(), "input data");

        let record_value: RecordData = raw_input.into();
        let entries = vec![Record::new_key_value(RecordKey::NULL, record_value)];

        let output = smartmodule
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

    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        _out: Arc<O>,
        _fluvio: &Fluvio,
    ) -> Result<()> {
        Ok(())
    }
}

//  target/release/fluvio sm test --input ww --regex "[A-Z]" --wasm-file target/wasm32-unknown-unknown/release-lto/fluvio_wasm_component.wasm
