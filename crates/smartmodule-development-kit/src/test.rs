use std::{collections::BTreeMap, path::PathBuf};

use std::fmt::Debug;

use bytes::Bytes;
use clap::Parser;
use anyhow::{self, Result};
use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use tracing::debug;

use fluvio_smartengine::{SmartEngine, SmartModuleChainBuilder, SmartModuleConfig};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;
use fluvio_protocol::record::Record;
use fluvio_cli_common::user_input::{UserInputRecords, UserInputType};

use crate::package::{PackageInfo, PackageOption};

/// Test SmartModule
#[derive(Debug, Parser)]
pub struct TestOpt {
    /// Provide test input with this flag
    #[clap(long, group = "TestInput")]
    text: Option<String>,

    /// Path to test file. Default: Read file line by line
    #[clap(long, groups = ["TestInput", "TestFile"])]
    file: Option<PathBuf>,

    /// Read the file as single record
    #[clap(long, action, requires = "TestFile")]
    raw: bool,

    /// Key to use with the test record(s)
    key: Option<String>,

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
        let mut chain_builder = SmartModuleChainBuilder::default();
        chain_builder.add_smart_module(
            SmartModuleConfig::builder().params(param.into()).build()?,
            raw,
        );

        debug!("SmartModule chain created");

        let mut chain = chain_builder.initialize(&engine)?;

        let key = self.key.map(Bytes::from);

        let test_data: UserInputRecords = if let Some(data) = self.text {
            UserInputRecords::try_from(UserInputType::Text {
                key,
                data: Bytes::from(data),
            })?
        } else if let Some(test_file_path) = &self.file {
            let path = test_file_path.to_path_buf();
            if self.raw {
                UserInputRecords::try_from(UserInputType::File { key, path })?
            } else {
                UserInputRecords::try_from(UserInputType::FileByLine { key, path })?
            }
        } else {
            return Err(anyhow::anyhow!("No valid input provided"));
        };
        debug!(len = &test_data.len(), "input data");

        let metrics = SmartModuleChainMetrics::default();

        let test_records: Vec<Record> = test_data.into();

        let output = chain.process(SmartModuleInput::try_from(test_records)?, &metrics)?;

        println!("{:?} records outputed", output.successes.len());
        for output_record in output.successes {
            let output_value = output_record.value.as_str()?;
            println!("{}", output_value);
        }

        Ok(())
    }
}
