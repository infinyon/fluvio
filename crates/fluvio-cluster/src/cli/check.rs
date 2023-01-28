use semver::Version;
use clap::Parser;

use crate::progress::ProgressBarFactory;
use crate::ClusterChecker;
use crate::check::{SysChartCheck, ClusterCheckError};
use crate::charts::ChartConfig;

#[derive(Debug, Parser)]
pub struct CheckOpt {
    /// Attempt to fix recoverable errors
    #[clap(long)]
    fix: bool,
}

impl CheckOpt {
    pub async fn process(self, platform_version: Version) -> Result<(), ClusterCheckError> {
        use colored::*;
        println!("{}", "Running pre-startup checks...".bold());
        println!(
            "{}",
            "Note: This may require admin access to current Kubernetes context"
                .bold()
                .yellow()
        );
        let sys_config: ChartConfig = ChartConfig::sys_builder()
            .build()
            .map_err(|err| ClusterCheckError::Other(format!("chart config error: {err:#?}")))?;
        let pb = ProgressBarFactory::new(false);
        ClusterChecker::empty()
            .with_preflight_checks()
            .with_check(SysChartCheck::new(sys_config, platform_version))
            .run(&pb, self.fix)
            .await?;

        Ok(())
    }
}
