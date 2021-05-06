use structopt::StructOpt;
use semver::Version;

use crate::{ClusterChecker, SysConfig, ClusterError};
use crate::cli::ClusterCliError;
use crate::check::render::{render_check_progress, render_results_next_steps};
use crate::check::SysChartCheck;

#[derive(Debug, StructOpt)]
pub struct CheckOpt {}

impl CheckOpt {
    pub async fn process(self, default_chart_version: Version) -> Result<(), ClusterCliError> {
        use colored::*;
        println!("{}", "Running pre-startup checks...".bold());
        let sys_config: SysConfig = SysConfig::builder(default_chart_version)
            .build()
            .map_err(ClusterError::InstallSys)?;
        let mut progress = ClusterChecker::empty()
            .with_preflight_checks()
            .with_check(SysChartCheck::new(sys_config))
            .run_with_progress();

        let results = render_check_progress(&mut progress).await;
        render_results_next_steps(&results);
        Ok(())
    }
}
