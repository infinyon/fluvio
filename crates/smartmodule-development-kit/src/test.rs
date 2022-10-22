use std::{collections::BTreeMap, path::PathBuf};

use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;
use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use tracing::debug;

use fluvio::RecordKey;
use fluvio_protocol::record::{RecordData, Record};
use fluvio_smartengine::{SmartEngine, SmartModuleConfig};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;

use crate::package::{PackageInfo, PackageOption};

/// Test SmartModule
#[derive(Debug, Parser)]
pub struct TestOpt {
    // text input
    #[clap(long)]
    text: Option<String>,

    // arbitrary file input
    #[clap(long)]
    file: Option<PathBuf>,

    #[clap(flatten)]
    package: PackageOption,

    /// Optional wasm file path
    #[clap(long)]
    wasm_file: Option<PathBuf>,

    /// (Optional) Extra input parameters passed to the smartmodule module.
    /// They should be passed using key=value format
    /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1
    #[clap(
        short = 'e',
        long= "params",
        value_parser=parse_key_val,
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
    pub(crate) fn process(self) -> Result<()> {
        debug!("starting smart module test");

        let raw = match &self.wasm_file {
            Some(wasm_file) => crate::read_bytes_from_path(wasm_file)?,
            None => PackageInfo::from_options(&self.package)
                .map_err(|e| anyhow::anyhow!(e))?
                .read_bytes()?,
        };

        let param: BTreeMap<String, String> = self.params.into_iter().collect();

        let engine = SmartEngine::new();
        let mut chain_builder = engine.builder();
        chain_builder.add_smart_module(
            SmartModuleConfig::builder().params(param.into()).build()?,
            raw,
        )?;

        debug!("SmartModule chain created");

        let mut chain = chain_builder.initialize()?;

        // get raw json in one of other ways
        let raw_input = if let Some(input) = self.text {
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

        let metrics = SmartModuleChainMetrics::default();
        let output = chain.process(SmartModuleInput::try_from(entries)?, &metrics)?;

        println!("{:?} records outputed", output.successes.len());
        for output_record in output.successes {
            let output_value = output_record.value.as_str()?;
            println!("{}", output_value);
        }

        Ok(())
    }
}
