use anyhow::bail;
use anyhow::Result;
use fluvio_extension_common::installation::InstallationType;
use semver::Version;
use clap::Parser;
use tracing::debug;

use crate::progress::ProgressBarFactory;
use crate::{ClusterChecker, cli::get_installation_type};
use crate::check::{SysChartCheck, ClusterCheckError};
use crate::charts::ChartConfig;

#[derive(Debug, Parser)]
pub struct CheckOpt {
    /// Attempt to fix recoverable errors
    #[arg(long)]
    fix: bool,
}

impl CheckOpt {
    pub async fn process(self, platform_version: Version) -> Result<()> {
        use colored::*;
        println!("{}", "Running pre-startup checks...".bold());
        println!(
            "{}",
            "Note: This may require admin access to current Kubernetes context"
                .bold()
                .yellow()
        );
        let (installation_ty, config) = get_installation_type()?;
        debug!(?installation_ty);

        let checker = match installation_ty {
            InstallationType::K8 => {
                let sys_config: ChartConfig =
                    ChartConfig::sys_builder().build().map_err(|err| {
                        ClusterCheckError::Other(format!("chart config error: {err:#?}"))
                    })?;
                ClusterChecker::empty()
                    .with_preflight_checks()
                    .with_check(SysChartCheck::new(sys_config, platform_version))
            }
            InstallationType::Local | InstallationType::ReadOnly => {
                ClusterChecker::empty().with_no_k8_checks()
            }
            InstallationType::LocalK8 => ClusterChecker::empty().with_local_checks(),
            InstallationType::Cloud => {
                let profile = config.config().current_profile_name().unwrap_or("none");
                bail!("'fluvio cluster check' is invalid with cloud cluster \"{profile}\", use 'fluvio cloud ...' commands");
            }

            _other => ClusterChecker::empty(),
        };

        let pb = ProgressBarFactory::new(false);

        checker.run(&pb, self.fix).await?;

        Ok(())
    }
}
