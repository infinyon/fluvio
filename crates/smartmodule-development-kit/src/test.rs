use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::io;
use std::path::PathBuf;

use anyhow::{Result, Context, anyhow};
use bytes::Bytes;
use cargo_builder::package::PackageInfo;
use clap::Parser;
use chrono::Utc;
use tracing::debug;

use fluvio::FluvioConfig;
use fluvio_sc_schema::smartmodule::SmartModuleApiClient;
use fluvio_smartengine::DEFAULT_SMARTENGINE_VERSION;
use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use fluvio_smartengine::transformation::TransformationConfig;
use fluvio_smartengine::{
    SmartEngine, SmartModuleChainBuilder, SmartModuleConfig, SmartModuleChainInstance, Lookback,
};
use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInput;
use fluvio_protocol::record::Record;
use fluvio_cli_common::user_input::{UserInputRecords, UserInputType};

use crate::cmd::PackageCmd;

/// Test SmartModule
#[derive(Debug, Parser)]
pub struct TestCmd {
    /// Provide test input with this flag
    #[arg(long, group = "TestInput")]
    text: Option<String>,

    /// Read the test input from the StdIn (e.g. Unix piping)
    #[arg(long, group = "TestInput")]
    stdin: bool,

    /// Path to test file. Default: Read file line by line
    #[arg(long, groups = ["TestInput", "TestFile"])]
    file: Option<PathBuf>,

    /// Read the file as single record
    #[arg(long, requires = "TestFile")]
    raw: bool,

    /// Key to use with the test record(s)
    key: Option<String>,

    /// Print records in "[key] value" format, with "[null]" for no key
    #[arg(short, long)]
    key_value: bool,

    #[clap(flatten)]
    package: PackageCmd,

    /// Optional wasm file path
    #[arg(long, group = "TestSmartModule")]
    wasm_file: Option<PathBuf>,

    /// (Optional) Extra input parameters passed to the smartmodule module.
    /// They should be passed using key=value format
    /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1
    #[arg(
        short = 'e',
        long= "params",
        value_parser=parse_key_val,
        num_args = 1,
        conflicts_with_all = ["transforms_file", "transform"]
    )]
    params: Vec<(String, String)>,

    /// (Optional) File path to transformation specification.
    #[arg(long, group = "TestSmartModule")]
    transforms_file: Option<PathBuf>,

    /// (Optional) Pass transformation specification as JSON formatted string.
    /// E.g. smdk test --text '{}' --transform='{"uses":"infinyon/jolt@0.1.0","with":{"spec":"[{\"operation\":\"default\",\"spec\":{\"source\":\"test\"}}]"}}'
    #[arg(long, short, group = "TestSmartModule")]
    transform: Vec<String>,

    /// verbose output
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,

    /// Records which act as existing in the topic before the SmartModule starts processing. Useful
    /// for testing `lookback`. Multiple values are allowed.
    #[arg(long, short)]
    record: Vec<String>,

    /// Sets the lookback parameter to the last N records.
    #[arg(long, short)]
    lookback_last: Option<u64>,
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!(format!("invalid KEY=value: no `=` found in `{s}`")))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

impl TestCmd {
    pub(crate) fn process(self) -> Result<()> {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { self.process_async().await })
    }

    async fn process_async(self) -> Result<()> {
        debug!("starting smartmodule test");

        let lookback: Option<Lookback> = self.lookback_last.map(Lookback::Last);

        let chain_builder = if let Some(transforms_file) = self.transforms_file {
            let config = TransformationConfig::from_file(transforms_file)
                .context("unable to read transformation config")?;
            build_chain(config, lookback).await?
        } else if !self.transform.is_empty() {
            let config = TransformationConfig::try_from(self.transform)
                .context("unable to parse transform")?;
            build_chain(config, lookback).await?
        } else if let Some(wasm_file) = self.wasm_file {
            build_chain_ad_hoc(
                crate::read_bytes_from_path(&wasm_file)?,
                self.params,
                lookback,
            )?
        } else {
            let package_info = PackageInfo::from_options(&self.package.as_opt())?;
            let wasm_file = package_info.target_wasm32_path()?;
            build_chain_ad_hoc(
                crate::read_bytes_from_path(&wasm_file)?,
                self.params,
                lookback,
            )?
        };

        let engine = SmartEngine::new();
        debug!("SmartModule chain created");

        let mut chain = chain_builder.initialize(&engine)?;
        look_back(&mut chain, self.record).await?;

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
        } else if self.stdin {
            let mut buf = String::new();
            io::stdin().read_line(&mut buf)?;
            UserInputRecords::try_from(UserInputType::StdIn {
                key,
                data: buf.into(),
            })?
        } else {
            return Err(anyhow::anyhow!("No valid input provided"));
        };
        debug!(len = &test_data.len(), "input data");

        let metrics = SmartModuleChainMetrics::default();

        let test_records: Vec<Record> = test_data.into();
        let mut sm_input =
            SmartModuleInput::try_from_records(test_records, DEFAULT_SMARTENGINE_VERSION)?;

        sm_input.set_base_timestamp(Utc::now().timestamp_millis());

        let output = chain.process(sm_input, &metrics)?;

        if self.verbose {
            println!("{:?} records outputed", output.successes.len());
        }
        for output_record in output.successes {
            let output_value = if self.key_value {
                format!(
                    "[{formatted_key}] {value}",
                    formatted_key = if let Some(key) = output_record.key() {
                        key.to_string()
                    } else {
                        "null".to_string()
                    },
                    value = output_record.value.as_str()?
                )
            } else {
                output_record.value.as_str()?.to_string()
            };

            println!("{output_value}");
        }

        Ok(())
    }
}

async fn look_back(chain: &mut SmartModuleChainInstance, records: Vec<String>) -> Result<()> {
    let records: Vec<Record> = records
        .into_iter()
        .map(|r| Record::new(r.as_str()))
        .collect();
    chain
        .look_back(
            |lookback| {
                let n = match lookback {
                    fluvio_smartengine::Lookback::Last(n) => n,
                    fluvio_smartengine::Lookback::Age { age: _, last } => last,
                };
                let res = Ok(records
                    .clone()
                    .into_iter()
                    .rev()
                    .take(n as usize)
                    .rev()
                    .collect());
                async { res }
            },
            &Default::default(),
        )
        .await
}

async fn build_chain(
    config: TransformationConfig,
    lookback: Option<Lookback>,
) -> Result<SmartModuleChainBuilder> {
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
        let mut config = SmartModuleConfig::from(transform);
        config.set_lookback(lookback);
        chain_builder.add_smart_module(config, wasm);
    }
    Ok(chain_builder)
}

fn build_chain_ad_hoc(
    wasm: Vec<u8>,
    params: Vec<(String, String)>,
    lookback: Option<Lookback>,
) -> Result<SmartModuleChainBuilder> {
    let params: BTreeMap<String, String> = params.into_iter().collect();
    Ok(SmartModuleChainBuilder::from((
        SmartModuleConfig::builder()
            .params(params.into())
            .lookback(lookback)
            .build()?,
        wasm,
    )))
}
