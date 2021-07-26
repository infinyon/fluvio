use std::path::{PathBuf};


use tracing::{info,instrument};
use derive_builder::Builder;

use semver::Version;

use fluvio_helm::{HelmClient, InstallArg};

use crate::DEFAULT_NAMESPACE;

use super::ChartInstallError;
use super::location::ChartLocation;
use super::SYS_CHART_NAME;

const APP_CHART_NAME: &str = "fluvio";
const DEFAULT_CHART_REMOTE: &str = "https://charts.fluvio.io";


/// Configuration options for installing Fluvio system charts
#[derive(Builder, Debug, Clone)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct ChartConfig {
    
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
    pub location: ChartLocation,
    /// The version of the chart to install (REQUIRED).
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
    pub version: Version,

    #[builder(setter(into))]
    pub name: String,


    /// Set a list of chart value paths.
    #[builder(default)]
    values: Vec<PathBuf>,

    /// inline array of string values
    /// equavalent to helm set
    #[builder(default)]
    string_values: Vec<(String,String)>

}

impl ChartConfig {
    /// Creates builder for app chart 
    ///
    /// The required argument `chart_version` must be provdied when
    /// constructing the builder.
    ///
    /// # Example
    ///
    /// ```
    /// use fluvio_cluster::ChartConfig;
    /// use semver::Version;
    /// let builder = ChartConfig::app_builder(Version::parse("0.7.0-alpha.1").unwrap());
    /// ```
    pub fn app_builder(chart_version: Version) -> ChartConfigBuilder {
        let mut builder = ChartConfigBuilder::default();
        builder.version(chart_version);
        builder.name(APP_CHART_NAME);
        builder.location(ChartLocation::app_inline());
        builder
    }

    pub fn sys_builder(chart_version: Version) -> ChartConfigBuilder {
        let mut builder = ChartConfigBuilder::default();
        builder.version(chart_version);
        builder.name(SYS_CHART_NAME);
        builder.location(ChartLocation::app_inline());
        builder
    }
}

impl ChartConfigBuilder {
    /// Validates all builder options and constructs a `SysConfig`
    pub fn build(&self) -> Result<ChartConfig, ChartInstallError> {
        let config = self
            .build_impl()
            .map_err(ChartInstallError::MissingRequiredConfig)?;
        Ok(config)
    }

    /// The local chart location to install sys charts from
    pub fn local<P: Into<PathBuf>>(&mut self, path: P) -> &mut Self {
        self.location = Some(ChartLocation::Local(path.into()));
        self
    }

    /// The remote chart location to install charts from
    pub fn remote<S: Into<String>>(&mut self, location: S) -> &mut Self {
        self.location = Some(ChartLocation::Remote(location.into()));
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
    /// let config = ChartConfig::builder(Version::parse("0.7.0-alpha.1").unwrap())
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
/// let config = ChartConfig::builder(Version::parse("0.7.0-alpha.1").unwrap(),"fluvio-app")
///     .namespace("fluvio")
///     .build()?;
/// let installer = SysInstaller::from_config(config)?;
/// installer.install()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ChartInstaller {
    config: ChartConfig,
    helm_client: HelmClient,
}

impl ChartInstaller {
    /// Create a new `SysInstaller` using the given config
    pub fn from_config(config: ChartConfig) -> Result<Self, ChartInstallError> {
        let helm_client = HelmClient::new()?;
        Ok(Self {
            config,
            helm_client,
        })
    }

    /// Install the Fluvio System chart on the configured cluster
    pub fn install(&self) -> Result<(), ChartInstallError> {
        self.process(false)
    }

    /// Upgrade the Fluvio System chart on the configured cluster
    pub fn upgrade(&self) -> Result<(), ChartInstallError> {
        self.process(true)
    }

    /// Tells whether a chart is installed the configured details is already installed
    pub fn is_installed(&self) -> Result<bool, ChartInstallError> {
        let installed_charts = self
            .helm_client
            .get_installed_chart_by_name(&self.config.name, None)?;
        Ok(!installed_charts.is_empty())
    }

    #[instrument(skip(self))]
    pub fn process(
        &self, 
        upgrade: bool,
    ) -> Result<(), ChartInstallError> {
        
        let chart_setup = self.config.location.setup(&self.config.name,&self.helm_client)?;

        let args = InstallArg::new( &chart_setup.location(),&self.config.name)
            .namespace(&self.config.namespace)
            .version(&self.config.version.to_string())
            .opts(self.config.string_values)
            .values(self.config.values)
            .develop();
        if upgrade {
            self.helm_client.upgrade(&args)?;
        } else {
            self.helm_client.install(&args)?;
        }

        info!(chart = %self.config.name," has been installed");
        Ok(())
    }


}




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_config() {
        let config: ChartConfig =
            ChartConfig::app_builder(semver::Version::parse("0.7.0-alpha.1").unwrap())
                .build()
                .expect("should build config with required options");
        assert_eq!(
            config.version,
            semver::Version::parse("0.7.0-alpha.1").unwrap()
        );
    }
}
