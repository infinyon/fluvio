use tracing::{info, debug, instrument};
use derive_builder::Builder;
use crate::{
    ChartLocation, DEFAULT_CHART_APP_REPO, DEFAULT_CHART_SYS_REPO, DEFAULT_NAMESPACE,
    DEFAULT_CHART_REMOTE,
};
use fluvio_helm::{HelmClient, InstallArg};
use std::path::{Path, PathBuf};
use crate::error::SysInstallError;

const DEFAULT_SYS_NAME: &str = "fluvio-sys";
const DEFAULT_CHART_SYS_NAME: &str = "fluvio/fluvio-sys";
const DEFAULT_CLOUD_NAME: &str = "minikube";

/// Configuration options for installing Fluvio system charts
#[derive(Builder, Debug, Clone)]
#[builder(build_fn(skip), setter(prefix = "with"))]
pub struct SysConfig {
    /// The type of cloud infrastructure the cluster will be running on
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn add_cloud(builder: &mut SysConfigBuilder) {
    /// builder.with_cloud("minikube");
    /// # }
    /// ```
    #[builder(setter(into))]
    pub cloud: String,
    /// The namespace in which to install the system chart
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn add_namespace(builder: &mut SysConfigBuilder) {
    /// builder.with_namespace("fluvio");
    /// # }
    /// ```
    #[builder(setter(into))]
    pub namespace: String,
    /// The location at which to find the system chart to install
    pub chart_location: ChartLocation,
    /// The version of the system chart to install (REQUIRED).
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn example(builder: &mut SysConfigBuilder) {
    /// builder.with_chart_version("0.6.1");
    /// # }
    /// ```
    #[builder(setter(into))]
    pub chart_version: String,
}

impl SysConfig {
    /// Creates a default [`SysConfigBuilder`].
    ///
    /// # Example
    ///
    /// ```
    /// use fluvio_cluster::SysConfig;
    /// let builder = SysConfig::builder();
    /// ```
    pub fn builder() -> SysConfigBuilder {
        SysConfigBuilder::default()
    }
}

impl SysConfigBuilder {
    /// Validates all builder options and constructs a `SysConfig`
    pub fn build(&self) -> Result<SysConfig, SysInstallError> {
        let cloud = self
            .cloud
            .clone()
            .unwrap_or_else(|| DEFAULT_CLOUD_NAME.to_string());
        let namespace = self
            .namespace
            .clone()
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());
        let chart_location = self
            .chart_location
            .clone()
            .unwrap_or_else(|| ChartLocation::Remote(DEFAULT_CHART_REMOTE.to_string()));
        let chart_version = self
            .chart_version
            .clone()
            .ok_or_else(|| SysInstallError::MissingRequiredConfig("chart_version".to_string()))?;

        Ok(SysConfig {
            cloud,
            namespace,
            chart_location,
            chart_version,
        })
    }

    /// The local chart location to install sys charts from
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn add_local_chart(builder: &mut SysConfigBuilder) {
    /// builder.with_local_chart("./helm/fluvio-sys");
    /// # }
    /// ```
    pub fn with_local_chart<P: Into<PathBuf>>(&mut self, path: P) -> &mut Self {
        self.chart_location = Some(ChartLocation::Local(path.into()));
        self
    }

    /// The remote chart location to install sys charts from
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn add_remote_chart(builder: &mut SysConfigBuilder) {
    /// builder.with_remote_chart("https://charts.fluvio.io");
    /// # }
    /// ```
    pub fn with_remote_chart<S: Into<String>>(&mut self, location: S) -> &mut Self {
        self.chart_location = Some(ChartLocation::Remote(location.into()));
        self
    }

    /// A builder helper for conditionally setting options
    ///
    /// This is useful for maintaining a fluid call chain even when
    /// we only want to set certain options conditionally and the
    /// conditions are more complicated than a simple boolean.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{SysConfig, SysInstallError};
    /// enum NamespaceCandidate {
    ///     UserGiven(String),
    ///     System,
    ///     Default,
    /// }
    /// fn make_config(ns: NamespaceCandidate) -> Result<SysConfig, SysInstallError> {
    ///     let config = SysConfig::builder()
    ///         .with(|builder| match &ns {
    ///             NamespaceCandidate::UserGiven(user) => builder.with_namespace(user),
    ///             NamespaceCandidate::System => builder.with_namespace("system"),
    ///             NamespaceCandidate::Default => builder,
    ///         })
    ///         .build()?;
    ///     Ok(config)
    /// }
    /// ```
    pub fn with<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn(&mut Self) -> &mut Self,
    {
        f(self)
    }

    /// A builder helper for conditionally setting options
    ///
    /// This is useful for maintaining a builder call chain even when you
    /// only want to apply some options conditionally based on a boolean value.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{SysInstallError, SysConfig};
    /// # fn example() -> Result<(), SysInstallError> {
    /// let custom_namespace = false;
    /// let config = SysConfig::builder()
    ///     // Custom namespace is not applied
    ///     .with_if(custom_namespace, |builder| builder.with_namespace("my-namespace"))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_if<F>(&mut self, cond: bool, f: F) -> &mut Self
    where
        F: Fn(&mut Self) -> &mut Self,
    {
        if cond {
            f(self)
        } else {
            self
        }
    }
}

/// Installs or upgrades the Fluvio system charts
#[derive(Debug)]
pub struct SysInstaller {
    config: SysConfig,
    helm_client: HelmClient,
}

impl SysInstaller {
    /// Create a new `SysInstaller` using the default config
    pub fn new() -> Result<Self, SysInstallError> {
        let config = SysConfig::builder().build()?;
        Self::with_config(config)
    }

    /// Create a new `SysInstaller` using the given config
    pub fn with_config(config: SysConfig) -> Result<Self, SysInstallError> {
        let helm_client = HelmClient::new()?;
        Ok(Self {
            config,
            helm_client,
        })
    }

    /// Install the Fluvio System chart on the configured cluster
    pub fn install(&self) -> Result<(), SysInstallError> {
        self.process(false)
    }

    /// Upgrade the Fluvio System chart on the configured cluster
    pub fn upgrade(&self) -> Result<(), SysInstallError> {
        self.process(true)
    }

    /// Tells whether a system chart with the configured details is already installed
    pub fn is_installed(&self) -> Result<bool, SysInstallError> {
        let sys_charts = self
            .helm_client
            .get_installed_chart_by_name(DEFAULT_CHART_SYS_REPO, None)?;
        Ok(!sys_charts.is_empty())
    }

    #[instrument(skip(self))]
    fn process(&self, upgrade: bool) -> Result<(), SysInstallError> {
        let settings = vec![("cloud".to_owned(), self.config.cloud.to_owned())];
        match &self.config.chart_location {
            ChartLocation::Remote(chart_location) => {
                self.process_remote_chart(chart_location, upgrade, settings)?;
            }
            ChartLocation::Local(chart_home) => {
                self.process_local_chart(chart_home, upgrade, settings)?;
            }
        }

        info!("Fluvio sys chart has been installed");
        Ok(())
    }

    #[instrument(skip(self, upgrade))]
    fn process_remote_chart(
        &self,
        chart: &str,
        upgrade: bool,
        settings: Vec<(String, String)>,
    ) -> Result<(), SysInstallError> {
        debug!(?chart, "Using remote helm chart:");
        self.helm_client.repo_add(DEFAULT_CHART_APP_REPO, chart)?;
        self.helm_client.repo_update()?;
        let args = InstallArg::new(DEFAULT_CHART_SYS_REPO, DEFAULT_CHART_SYS_NAME)
            .namespace(&self.config.namespace)
            .version(&self.config.chart_version)
            .opts(settings)
            .develop();
        if upgrade {
            self.helm_client.upgrade(&args)?;
        } else {
            self.helm_client.install(&args)?;
        }
        Ok(())
    }

    #[instrument(skip(self, upgrade))]
    fn process_local_chart(
        &self,
        chart_home: &Path,
        upgrade: bool,
        settings: Vec<(String, String)>,
    ) -> Result<(), SysInstallError> {
        let chart_location = chart_home.join(DEFAULT_SYS_NAME);
        let chart_string = chart_location.to_string_lossy();
        debug!(chart_location = %chart_location.display(), "Using local helm chart:");

        let args = InstallArg::new(DEFAULT_CHART_SYS_REPO, chart_string)
            .namespace(&self.config.namespace)
            .version(&self.config.chart_version)
            .develop()
            .opts(settings);
        if upgrade {
            self.helm_client.upgrade(&args)?;
        } else {
            self.helm_client.install(&args)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let _config = SysConfig::builder()
            .with_namespace("fluvio")
            .with_cloud("minikube")
            .with_local_chart("./helm/fluvio-sys")
            .build()
            .expect("should build config");
    }
}
