use std::convert::TryInto;
use std::fmt::Debug;
use std::io;
use std::path::PathBuf;

use anyhow::{Result, Context, anyhow};
use bytes::Bytes;
use clap::Args;
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
use crate::user_input::{UserInputRecords, UserInputType};

/// Test SmartModule
#[derive(Debug, Args)]
pub struct BaseTestCmd {
    /// Provide test input with this flag
    #[arg(long, group = "TestInput")]
    pub text: Option<String>,

    /// Read the test input from the StdIn (e.g. Unix piping)
    #[arg(long, group = "TestInput")]
    pub stdin: bool,

    /// Path to test file. Default: Read file line by line
    #[arg(long, groups = ["TestInput", "TestFile"])]
    pub file: Option<PathBuf>,

    /// Read the file as single record
    #[arg(long, requires = "TestFile")]
    pub raw: bool,

    /// Key to use with the test record(s)
    pub key: Option<String>,

    /// Print records in "[key] value" format, with "[null]" for no key
    #[arg(short, long)]
    pub key_value: bool,

    /// (Optional) Extra input parameters passed to the smartmodule module.
    /// They should be passed using key=value format
    /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1
    #[arg(
        short = 'e',
        long= "params",
        value_parser=parse_key_val,
        num_args = 1,
        conflicts_with_all = ["transforms", "transforms_line"]
    )]
    pub params: Vec<(String, String)>,

    /// (Optional) File path to transformation specification.
    #[arg(short, long, group = "TestSmartModule", alias = "transforms-file")]
    pub transforms: Option<PathBuf>,

    /// (Optional) Pass transformation specification as JSON formatted string.
    /// E.g. smdk test --text '{}' --transforms-line='{"uses":"infinyon/jolt@0.1.0","with":{"spec":"[{\"operation\":\"default\",\"spec\":{\"source\":\"test\"}}]"}}'
    #[arg(long, group = "TestSmartModule", alias = "transform")]
    pub transforms_line: Vec<String>,

    /// verbose output
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,

    /// Records which act as existing in the topic before the SmartModule starts processing. Useful
    /// for testing `lookback`. Multiple values are allowed.
    #[arg(long, short)]
    pub record: Vec<String>,

    /// Sets the lookback parameter to the last N records.
    #[arg(long, short)]
    pub lookback_last: Option<u64>,
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let pos = s
        .find('=')
        .ok_or_else(|| anyhow::anyhow!(format!("invalid KEY=value: no `=` found in `{s}`")))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

impl BaseTestCmd {
    pub async fn process<F>(self, with_chain_builder: WithChainBuilder<F>) -> Result<()>
    where
        F: FnOnce(Option<Lookback>, Vec<(String, String)>) -> Result<SmartModuleChainBuilder>,
    {
        debug!("starting smartmodule test");

        let chain_builder = with_chain_builder
            .build(
                self.lookback_last,
                self.transforms,
                self.transforms_line,
                self.params,
            )
            .await?;

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
            println!("{:?} records outputted", output.successes.len());
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

#[derive(Debug)]
pub struct WithChainBuilder<F> {
    func: Option<F>,
}

impl<F> Default for WithChainBuilder<F> {
    fn default() -> Self {
        Self {
            func: Default::default(),
        }
    }
}

impl<F> WithChainBuilder<F>
where
    F: FnOnce(Option<Lookback>, Vec<(String, String)>) -> Result<SmartModuleChainBuilder>,
{
    async fn build(
        self,
        lookback_last: Option<u64>,
        transforms: Option<PathBuf>,
        transform: Vec<String>,
        params: Vec<(String, String)>,
    ) -> Result<SmartModuleChainBuilder> {
        let lookback = lookback_last.map(Lookback::Last);
        if let Some(transforms) = transforms {
            let config = TransformationConfig::from_file(transforms)
                .context("unable to read transformation config")?;
            build_chain(config, lookback).await
        } else if !transform.is_empty() {
            let config =
                TransformationConfig::try_from(transform).context("unable to parse transform")?;
            build_chain(config, lookback).await
        } else {
            debug_assert!(self.func.is_some(), "unknown condition");
            self.func.map(|f| f(lookback, params)).unwrap()
        }
    }

    pub fn extra_cond(mut self, func: F) -> Self {
        self.func = Some(func);
        self
    }
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
