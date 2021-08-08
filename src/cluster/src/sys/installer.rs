use std::path::{Path, PathBuf};

use tracing::{info, debug, instrument};
use derive_builder::Builder;
use semver::Version;

use crate::charts::{ChartConfig, ChartInstaller};
use crate::UserChartLocation;
use crate::DEFAULT_NAMESPACE;

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
    /// Creates a default [`SysConfigBuilder`].
    ///
    /// The required argument `chart_version` must be provdied when
    /// constructing the builder.
    ///
    /// # Example
    ///
    /// ```
    /// use fluvio_cluster::SysConfig;
    /// use semver::Version;
    /// let builder = SysConfig::builder(Version::parse("0.7.0-alpha.1").unwrap());
    /// ```
    pub fn builder(platform_version: Version) -> SysConfigBuilder {
        let mut builder = SysConfigBuilder::default();
        builder.platform_version(platform_version);
        builder
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
}

impl SysInstaller {
    /// Create a new `SysInstaller` using the given config
    pub fn from_config(config: SysConfig) -> Result<Self, SysInstallError> {
        Ok(Self { config })
    }

    /// run installation
    #[instrument(skip(self))]
    fn run(&self) -> Result<(), SysInstallError> {
        println!("Start Sys Configuration");

        let mut config = ChartConfig::sys_builder()
            .namespace(&self.config.namespace)
            .version(self.config.chart_version.clone())
            .build()?;

        let installer = ChartInstaller::from_config(config)?;

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
