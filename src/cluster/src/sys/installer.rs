use std::path::{ PathBuf};

use fluvio_helm::InstalledChart;
use tracing::{info, debug, instrument};
use derive_builder::Builder;
use semver::Version;

use crate::check::{CheckFailed, CheckResults, AlreadyInstalled, SysChartCheck};
use crate::charts::{ChartConfig, ChartInstaller,ChartInstallation};
use crate::UserChartLocation;
use crate::{ClusterError, StartStatus, DEFAULT_NAMESPACE, CheckStatus, ClusterChecker, CheckStatuses};


use super::SysInstallError;

/// Configuration options for installing Fluvio system charts
#[derive(Builder, Debug, Clone)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct SysConfig {
    /// Platform version
    #[builder(setter(into))]
    platform_version: Version,

    /// Sets the Kubernetes namespace to install Syste Chart.
    #[builder(setter(into), default = "DEFAULT_NAMESPACE.to_string()")]
    namespace: String,

    /// if true, don't perform install
    #[builder(default = "false")]
    dry_run: bool,

    /// Sets a specific version of the Fluvio helm chart to install.
    #[builder(setter(into), default)]
    chart_version: Option<Version>,
    /// The location to search for the Helm charts to install
    #[builder(setter(into, strip_option), default)]
    chart_location: Option<UserChartLocation>,
}

impl SysConfig {

    /// Creates a new builder [`SysConfigBuilder`].
    ///
    /// Required platform version
    pub fn builder(platform_version: Version) -> SysConfigBuilder {
        let mut builder = SysConfigBuilder::default();
        builder.platform_version(platform_version);
        builder
    }


    /// create sys installer
    async fn as_installer(self) -> Result<SysInstaller,SysInstallError> {

        self.check().await;
        SysInstaller::initialize_from(self)
    }

    /// perform system check to ensure system is ready
    /// 
    async fn check(&self) -> CheckResults {

        let checker = ClusterChecker::empty()
            .with_k8_checks();

        checker.run_wait_and_fix().await
        
    }
}

impl SysConfigBuilder {
    /// Validates all builder options and constructs a `SysConfig`
    pub fn build(&self) -> Result<SysConfig, SysInstallError> {
        let config = self
            .build_impl()
            .map_err(|err| SysInstallError::MissingRequiredConfig(err.to_string()))?;
        Ok(config)
    }

    /// The local chart location to install sys charts from
    pub fn local_chart<S: Into<PathBuf>>(&mut self, local_chart_location: S) -> &mut Self {
        let user_chart_location = UserChartLocation::Local(local_chart_location.into());
        debug!(?user_chart_location, "setting local chart");
        self.chart_location(user_chart_location);
        self
    }

    /// set remote chart
    pub fn remote_chart<S: Into<String>>(&mut self, remote_chart_location: S) -> &mut Self {
        self.chart_location(UserChartLocation::Remote(remote_chart_location.into()));
        self
    }
}

/// Installs or upgrades the Fluvio system components

#[derive(Debug)]
pub struct SysInstaller {
    config: SysConfig,
    chart_installer: ChartInstaller,
    sys_chart: Option<ChartInstallation>
}

impl SysInstaller {
    /// Create new SysInstaller and make it ready
    /// This requires we perform sanity check
    pub fn initialize_from(config: SysConfig) -> Result<Self, SysInstallError> {
        let chart_config = ChartConfig::sys_builder()
            .namespace(&config.namespace)
            .version(config.chart_version.clone())
            .build()?;

        let chart_installer = ChartInstaller::from_config(chart_config)?;

        // retrieve charts
        let mut sys_charts = chart_installer.retrieve_installations()?;
        // there should be only be 1 sys charts
        let sys_chart = if sys_charts.len() > 1 {
            return Err(SysInstallError::Other("there are more than 1 sys chart".to_owned()));
        } else {
            if let Some(chart) = sys_charts.pop() {
                Some(ChartInstallation::from(chart)?)
            } else {
                None
            }
        };

        Ok(Self {
            config,
            chart_installer,
            sys_chart

        })
    }

    /// is same version as our platform?
    pub fn is_sys_chart_same_version(&self) -> bool {

        /* 
        if let Some(chart) = self.sys_chart {
            chart.app_version() == self.config.platform_version
        } else {
            false
        }
        */
        false
    }

    /// run installation
    #[instrument(skip(self))]
    fn run(&self) -> Result<(), SysInstallError> {
        println!("Start System Installation");


        // first

        info!("Fluvio sys chart has been installed");
        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    /*
    #[test]
    fn test_build_config() {
        let config: SysConfig =
            SysConfig::builder(semver::Version::parse("0.7.0-alpha.1").unwrap())
                .build()
                .expect("should build config with required options");
        assert_eq!(
            config.chart_version,
            semver::Version::parse("0.7.0-alpha.1").unwrap()
        );
    }
    */
}
