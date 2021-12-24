
use fluvio::dataplane::versions::PlatformVersion;
use semver::Version;
use structopt::StructOpt;

use crate::{ClusterChecker, ClusterError};
use crate::cli::ClusterCliError;
use crate::check::render::{render_check_progress, render_results_next_steps};
use crate::check::SysChartCheck;
use crate::charts::ChartConfig;

#[derive(Debug, StructOpt)]
pub struct CheckOpt {}

impl CheckOpt {
    pub async fn process(self, platform_version: Version) -> Result<(), ClusterCliError> {
        use colored::*;
        println!("{}", "Running pre-startup checks...".bold());
        let sys_config: ChartConfig = ChartConfig::sys_builder()
            .build()
            .map_err(ClusterError::InstallSys)?;
        let mut progress = ClusterChecker::empty()
            .with_preflight_checks()
            .with_check(SysChartCheck::new(sys_config, platform_version))
            .run_with_progress();

        let results = render_check_progress(&mut progress).await;
        render_results_next_steps(&results);
        Ok(())
    }
}
