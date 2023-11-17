use std::fmt::Debug;
use std::path::PathBuf;

use anyhow::Result;
use cargo_builder::package::PackageInfo;
use clap::Parser;
use fluvio_future::task::run_block_on;
use fluvio_smartengine::{SmartModuleChainBuilder, SmartModuleConfig, Lookback};
use crate::cmd::PackageCmd;

use fluvio_cli_common::smartmodule::{BaseTestCmd, WithChainBuilder};
#[derive(Debug, Parser)]
#[command(arg_required_else_help = true)]
pub struct TestCmd {
    #[clap(flatten)]
    base_test_cmd: BaseTestCmd,
    #[clap(flatten)]
    package: PackageCmd,
    #[arg(long, group = "TestSmartModule")]
    wasm_file: Option<PathBuf>,
}

impl TestCmd {
    pub(crate) fn process(self) -> Result<()> {
        run_block_on(self.process_async())
    }

    async fn process_async(self) -> Result<()> {
        self.base_test_cmd
            .process(WithChainBuilder::default().extra_cond(|lookback, params| {
                let wasm_file = if let Some(wasm_file) = self.wasm_file {
                    wasm_file
                } else {
                    let package_info = PackageInfo::from_options(&self.package.as_opt())?;
                    package_info.target_wasm32_path()?
                };
                build_chain_ad_hoc(crate::read_bytes_from_path(&wasm_file)?, params, lookback)
            }))
            .await
    }
}

fn build_chain_ad_hoc(
    wasm: Vec<u8>,
    params: Vec<(String, String)>,
    lookback: Option<Lookback>,
) -> Result<SmartModuleChainBuilder> {
    use std::collections::BTreeMap;
    let params: BTreeMap<_, _> = params.into_iter().collect();
    Ok(SmartModuleChainBuilder::from((
        SmartModuleConfig::builder()
            .params(params.into())
            .lookback(lookback)
            .build()?,
        wasm,
    )))
}
