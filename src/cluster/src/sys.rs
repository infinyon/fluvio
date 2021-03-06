use tracing::{info, debug, instrument};
use derive_builder::Builder;
use crate::{
    ChartLocation, DEFAULT_CHART_APP_REPO, DEFAULT_CHART_SYS_REPO, DEFAULT_NAMESPACE,
    DEFAULT_CHART_REMOTE,
};
use fluvio_helm::{HelmClient, InstallArg};
use std::path::{Path, PathBuf};
use crate::error::SysInstallError;
use semver::Version;

const DEFAULT_SYS_NAME: &str = "fluvio-sys";
const DEFAULT_CHART_SYS_NAME: &str = "fluvio/fluvio-sys";
const DEFAULT_CLOUD_NAME: &str = "minikube";

/// Configuration options for installing Fluvio system charts
#[derive(Builder, Debug, Clone)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct SysConfig {
    /// The type of cloud infrastructure the cluster will be running on
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn add_cloud(builder: &mut SysConfigBuilder) {
    /// builder.cloud("minikube");
    /// # }
    /// ```
    #[builder(setter(into), default = "DEFAULT_CLOUD_NAME.to_string()")]
    pub cloud: String,
    /// The namespace in which to install the system chart
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn add_namespace(builder: &mut SysConfigBuilder) {
    /// builder.namespace("fluvio");
    /// # }
    /// ```
    #[builder(setter(into), default = "DEFAULT_NAMESPACE.to_string()")]
    pub namespace: String,
    /// The location at which to find the system chart to install
    #[builder(default = "ChartLocation::Remote(DEFAULT_CHART_REMOTE.to_string())")]
    pub chart_location: ChartLocation,
    /// The version of the system chart to install (REQUIRED).
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn example(builder: &mut SysConfigBuilder) {
    /// use semver::Version;
    /// builder.chart_version(Version::parse("0.6.1").unwrap());
    /// # }
    /// ```
    #[builder(setter(into))]
    pub chart_version: Version,
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
    pub fn builder(chart_version: Version) -> SysConfigBuilder {
        let mut builder = SysConfigBuilder::default();
        builder.chart_version(chart_version);
        builder
    }
}

impl SysConfigBuilder {
    /// Validates all builder options and constructs a `SysConfig`
    pub fn build(&self) -> Result<SysConfig, SysInstallError> {
        let config = self
            .build_impl()
            .map_err(SysInstallError::MissingRequiredConfig)?;
        Ok(config)
    }

    /// The local chart location to install sys charts from
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::SysConfigBuilder;
    /// # fn add_local_chart(builder: &mut SysConfigBuilder) {
    /// builder.local_chart("./helm/fluvio-sys");
    /// # }
    /// ```
    pub fn local_chart<P: Into<PathBuf>>(&mut self, path: P) -> &mut Self {
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
    /// builder.remote_chart("https://charts.fluvio.io");
    /// # }
    /// ```
    pub fn remote_chart<S: Into<String>>(&mut self, location: S) -> &mut Self {
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
    /// use semver::Version;
    /// enum NamespaceCandidate {
    ///     UserGiven(String),
    ///     System,
    ///     Default,
    /// }
    /// fn make_config(ns: NamespaceCandidate) -> Result<SysConfig, SysInstallError> {
    ///     let config = SysConfig::builder(Version::parse("0.7.0-alpha.1").unwrap())
    ///         .with(|builder| match &ns {
    ///             NamespaceCandidate::UserGiven(user) => builder.namespace(user),
    ///             NamespaceCandidate::System => builder.namespace("system"),
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
    /// use semver::Version;
    /// let custom_namespace = false;
    /// let config = SysConfig::builder(Version::parse("0.7.0-alpha.1").unwrap())
    ///     // Custom namespace is not applied
    ///     .with_if(custom_namespace, |builder| builder.namespace("my-namespace"))
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
///
/// # Example
///
/// ```
/// # use fluvio_cluster::{SysInstallError, SysConfig, SysInstaller};
/// # fn example() -> Result<(), SysInstallError> {
/// use semver::Version;
/// let config = SysConfig::builder(Version::parse("0.7.0-alpha.1").unwrap())
///     .namespace("fluvio")
///     .build()?;
/// let installer = SysInstaller::from_config(config)?;
/// installer.install()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SysInstaller {
    config: SysConfig,
    helm_client: HelmClient,
}

impl SysInstaller {
    /// Create a new `SysInstaller` using the given config
    pub fn from_config(config: SysConfig) -> Result<Self, SysInstallError> {
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
            .version(&self.config.chart_version.to_string())
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
            .version(&self.config.chart_version.to_string())
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
}
