use std::convert::TryInto;
use std::{collections::BTreeMap, path::PathBuf};

use std::fmt::Debug;

use bytes::Bytes;
use clap::Parser;
use anyhow::{Result, Context, anyhow};
use tracing::debug;

use cargo_builder::package::PackageInfo;
use fluvio::FluvioConfig;
use fluvio_future::task::run_block_on;
use fluvio_sc_schema::smartmodule::SmartModuleApiClient;
use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use fluvio_smartengine::transformation::TransformationConfig;
use fluvio_smartengine::{SmartEngine, SmartModuleChainBuilder, SmartModuleConfig};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;
use fluvio_protocol::record::Record;
use fluvio_cli_common::user_input::{UserInputRecords, UserInputType};

use crate::cmd::PackageCmd;

/// Test SmartModule
#[derive(Debug, Parser)]
pub struct TestCmd {
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
    package: PackageCmd,

    /// Optional wasm file path
    #[clap(long, group = "TestSmartModule")]
    wasm_file: Option<PathBuf>,

    /// (Optional) Extra input parameters passed to the smartmodule module.
    /// They should be passed using key=value format
    /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1
    #[clap(
        short = 'e',
        long= "params",
        value_parser=parse_key_val,
        number_of_values = 1,
        conflicts_with_all = ["transforms_file", "transform"]
    )]
    params: Vec<(String, String)>,

    /// (Optional) File path to transformation specification.
    #[clap(long, group = "TestSmartModule")]
    transforms_file: Option<PathBuf>,

    /// (Optional) Pass transformation specification as JSON formatted string.
    /// E.g. smdk test --text '{}' --transform='{"uses":"infinyon/jolt@0.1.0","with":{"spec":"[{\"operation\":\"default\",\"spec\":{\"source\":\"test\"}}]"}}'
    #[clap(long, short, group = "TestSmartModule")]
    transform: Vec<String>,
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!(format!("invalid KEY=value: no `=` found in `{s}`")))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

impl TestCmd {
    pub(crate) fn process(self) -> Result<()> {
        debug!("starting smartmodule test");

        let chain_builder = if let Some(transforms_file) = self.transforms_file {
            let config = TransformationConfig::from_file(transforms_file)
                .context("unable to read transformation config")?;
            run_block_on(build_chain(config))?
        } else if !self.transform.is_empty() {
            let config = TransformationConfig::try_from(self.transform)
                .context("unable to parse transform")?;
            run_block_on(build_chain(config))?
        } else if let Some(wasm_file) = self.wasm_file {
            build_chain_ad_hoc(crate::read_bytes_from_path(&wasm_file)?, self.params)?
        } else {
            let package_info = PackageInfo::from_options(&self.package.as_opt())?;
            let wasm_file = package_info.target_wasm32_path()?;
            build_chain_ad_hoc(crate::read_bytes_from_path(&wasm_file)?, self.params)?
        };

        let engine = SmartEngine::new();
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
            println!("{output_value}");
        }

        Ok(())
    }
}

async fn build_chain(config: TransformationConfig) -> Result<SmartModuleChainBuilder> {
    let client_config = FluvioConfig::load()?.try_into()?;
    let api_client = SmartModuleApiClient::connect_with_config(client_config).await?;
    let mut chain_builder = SmartModuleChainBuilder::default();
    for transform in config.transforms {
        debug!(?transform, "fetching");
        let wasm = api_client
            .get(transform.uses.clone())
            .await?
            .ok_or_else(|| anyhow!("smartmodule {} not found", &transform.uses))?
            .wasm
            .as_raw_wasm()?;
        let config = SmartModuleConfig::from(transform);
        chain_builder.add_smart_module(config, wasm);
    }
    Ok(chain_builder)
}

fn build_chain_ad_hoc(
    wasm: Vec<u8>,
    params: Vec<(String, String)>,
) -> Result<SmartModuleChainBuilder> {
    let params: BTreeMap<String, String> = params.into_iter().collect();
    Ok(SmartModuleChainBuilder::from((
        SmartModuleConfig::builder().params(params.into()).build()?,
        wasm,
    )))
}
