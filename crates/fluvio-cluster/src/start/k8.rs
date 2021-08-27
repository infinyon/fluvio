use std::io::Error as IoError;
use std::io::ErrorKind;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::borrow::Cow;
use std::process::Command;
use std::time::Duration;
use std::net::SocketAddr;
use std::env;

use derive_builder::Builder;
use tracing::{info, warn, debug, error, instrument};
use once_cell::sync::Lazy;
use tempfile::NamedTempFile;
use semver::Version;

use fluvio::{Fluvio, FluvioConfig};
use fluvio::metadata::spg::SpuGroupSpec;
use fluvio::metadata::spu::SpuSpec;
use fluvio::config::{TlsPolicy, TlsConfig, TlsPaths, ConfigFile, Profile};
use fluvio_future::timer::sleep;
use fluvio_future::net::{TcpStream, resolve};
use k8_client::K8Client;
use k8_config::K8Config;
use k8_client::meta_client::MetadataClient;
use k8_types::core::service::{LoadBalancerType, ServiceSpec, TargetPort};
use k8_types::core::node::{NodeSpec, NodeAddress};
use fluvio_command::CommandExt;

use crate::helm::{HelmClient};
use crate::check::{CheckFailed, CheckResults, AlreadyInstalled, SysChartCheck};
use crate::error::K8InstallError;
use crate::{ClusterError, StartStatus, DEFAULT_NAMESPACE, CheckStatus, ClusterChecker, CheckStatuses};
use crate::charts::{ChartConfig, ChartInstaller};
use crate::check::render::render_check_progress;
use crate::UserChartLocation;

use super::constants::*;

const DEFAULT_REGISTRY: &str = "infinyon";
const DEFAULT_GROUP_NAME: &str = "main";
const DEFAULT_SPU_REPLICAS: u16 = 1;
const DEFAULT_SERVICE_TYPE: &str = "NodePort";

const FLUVIO_SC_SERVICE: &str = "fluvio-sc-public";
/// maximum time waiting for sc service to come up
static MAX_SC_SERVICE_WAIT: Lazy<u64> = Lazy::new(|| {
    let var_value = env::var("FLV_CLUSTER_MAX_SC_SERVICE_WAIT").unwrap_or_default();
    var_value.parse().unwrap_or(60)
});

const NETWORK_SLEEP_MS: u64 = 2000;
/// maximum tiime for VERSION CHECK
static MAX_SC_VERSION_LOOP: Lazy<u8> = Lazy::new(|| {
    let var_value = env::var("FLV_CLUSTER_MAX_SC_VERSION_LOOP").unwrap_or_default();
    var_value.parse().unwrap_or(60)
});

/// Describes how to install Fluvio onto Kubernetes
#[derive(Builder, Debug)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct ClusterConfig {
    /// Platform version
    #[builder(setter(into))]
    platform_version: Version,
    /// Sets the Kubernetes namespace to install Fluvio into.
    ///
    /// The default namespace is "default".
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .namespace("my-namespace")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(setter(into), default = "DEFAULT_NAMESPACE.to_string()")]
    namespace: String,
    /// Sets the docker image tag of the Fluvio image to install.
    ///
    /// If this is not specified, the installer will use the chart version
    /// as the image tag. This should correspond to the image tags of the
    /// official published Fluvio images.
    ///
    /// # Example
    ///
    /// Suppose you would like to install version `0.6.0` of Fluvio from
    /// Docker Hub, where the image is tagged as `infinyon/fluvio:0.6.0`.
    /// You can do that like this:
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .image_tag("0.6.0")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(setter(into, strip_option), default)]
    image_tag: Option<String>,
    /// Sets the docker image registry to use to download Fluvio images.
    ///
    /// This defaults to `infinyon` to pull from Infinyon's official Docker Hub
    /// registry. This can be used to specify a private registry or a local
    /// registry as a source of Fluvio images.
    ///
    /// # Example
    ///
    /// You can create a local Docker registry to publish images to during
    /// development. Suppose you have a local registry running such as the following:
    ///
    /// ```bash
    /// docker run -d -p 5000:5000 --restart=always --name registry registry:2
    /// ```
    ///
    /// Suppose you tagged your image as `infinyon/fluvio:0.1.0` and pushed it
    /// to your `localhost:5000` registry. Your image is now located at
    /// `localhost:5000/infinyon`. You can specify that to the installer like so:
    ///
    /// > **NOTE**: See [`image_tag`] to see how to specify the `0.1.0` shown here.
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .image_registry("localhost:5000/infinyon")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Then, when you use `installer.install_fluvio()`, it will pull the images
    /// from your local docker registry.
    ///
    /// [`image_tag`]: ./struct.ClusterInstaller.html#method.image_tag
    #[builder(setter(into), default = "DEFAULT_REGISTRY.to_string()")]
    image_registry: String,
    /// Sets a specific version of the Fluvio helm chart to install.
    #[builder(setter(into), default)]
    chart_version: Option<Version>,
    /// The location to search for the Helm charts to install
    #[builder(setter(into, strip_option), default)]
    chart_location: Option<UserChartLocation>,
    /// Sets a custom SPU group name. The default is `main`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .group_name("orange")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(setter(into), default = "DEFAULT_GROUP_NAME.to_string()")]
    group_name: String,

    /// How many SPUs to provision for this Fluvio cluster. Defaults to 1
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .spu_replicas(2)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "DEFAULT_SPU_REPLICAS")]
    spu_replicas: u16,
    /// Sets the [`RUST_LOG`] environment variable for the installation.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .rust_log("debug")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`RUST_LOG`]: https://docs.rs/tracing-subscriber/0.2.11/tracing_subscriber/filter/struct.EnvFilter.html
    #[builder(setter(into, strip_option), default)]
    rust_log: Option<String>,
    /// The TLS policy for the SC and SPU servers
    #[builder(default = "TlsPolicy::Disabled")]
    server_tls_policy: TlsPolicy,
    /// The TLS policy for the client
    #[builder(default = "TlsPolicy::Disabled")]
    client_tls_policy: TlsPolicy,
    /// Set a list of chart value paths.
    #[builder(default)]
    chart_values: Vec<PathBuf>,
    /// Sets the ConfigMap for authorization.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .authorization_config_map("authorization")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    #[builder(setter(into, strip_option), default)]
    authorization_config_map: Option<String>,
    /// Whether to save a profile of this installation to `~/.fluvio/config`. Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .save_profile(true)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "false")]
    save_profile: bool,
    /// Whether to install the `fluvio-sys` chart in the full installation. Defaults to `true`.
    ///
    /// # Example
    ///
    /// If you want to disable installing the system chart, you can do this
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .install_sys(false)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "true")]
    install_sys: bool,
    /// Whether to update the `kubectl` context to match the Fluvio installation. Defaults to `true`.
    ///
    /// # Example
    ///
    /// If you do not want your Kubernetes contexts to be updated, you can do this
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .update_context(false)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "false")]
    update_context: bool,
    /// Whether to upgrade an existing installation
    #[builder(default = "false")]
    upgrade: bool,
    /// Whether to skip pre-install checks before installation. Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .skip_checks(true)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "false")]
    skip_checks: bool,
    /// Use cluster IP instead of load balancer for communication to SC
    ///
    /// This is is useful inside k8 cluster
    #[builder(default = "false")]
    use_cluster_ip: bool,
    /// Use NodePort instead of load balancer for SC and SPU
    #[builder(setter(into), default = "DEFAULT_SERVICE_TYPE.to_string()")]
    service_type: String,
    /// If set, skip spu liveness check
    #[builder(default = "false")]
    skip_spu_liveness_check: bool,
    /// Whether to render pre-install checks to stdout as they are performed.
    ///
    /// Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .render_checks(true)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "false")]
    render_checks: bool,

    /// Use proxy address for communicating with kubernetes cluster
    #[builder(setter(into), default)]
    proxy_addr: Option<String>,
}

impl ClusterConfig {
    /// Creates a default [`ClusterConfigBuilder`].
    ///
    /// The required option `chart_version` must be provided when constructing
    /// the builder.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::ClusterConfig;
    /// use semver::Version;
    /// let builder = ClusterConfig::builder(Version::parse("0.7.0-alpha.1").unwrap());
    /// ```
    pub fn builder(platform_version: Version) -> ClusterConfigBuilder {
        let mut builder = ClusterConfigBuilder::default();
        builder.platform_version(platform_version);
        builder
    }
}

impl ClusterConfigBuilder {
    /// Creates a [`ClusterConfig`] with the collected configuration options.
    ///
    /// This may fail if there are missing required configuration options.
    ///
    /// # Example
    ///
    /// The simplest flow to create a `ClusterConfig` is the following:
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// use semver::Version;
    /// let config = ClusterConfig::builder(Version::parse("0.7.0-alpha.1").unwrap()).build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`ClusterInstaller`]: ./struct.ClusterInstaller.html
    pub fn build(&self) -> Result<ClusterConfig, ClusterError> {
        let config = self
            .build_impl()
            .map_err(|err| K8InstallError::MissingRequiredConfig(err.to_string()))?;
        Ok(config)
    }

    /// Applies development options to this cluster configuration.
    ///
    /// The following options are applied when using this method.
    /// NOTE that these options will be overwritten even if they were
    /// previously assigned.
    ///
    /// - Use the git hash of HEAD as the image_tag
    pub fn development(&mut self) -> Result<&mut Self, ClusterError> {
        let output = Command::new("git")
            .args(&["rev-parse", "HEAD"])
            .result()
            .map_err(K8InstallError::CommandError)?;
        let git_hash = String::from_utf8(output.stdout).map_err(|e| {
            K8InstallError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!("failed to get git hash: {}", e),
            ))
        })?;
        self.image_tag(git_hash.trim());
        Ok(self)
    }

    /// Sets a local helm chart location to search for Fluvio charts.
    ///
    /// This is often desirable when developing for Fluvio locally and making
    /// edits to the chart. When using this option, the argument is expected to be
    /// a local filesystem path. The path given is expected to be the parent directory
    /// of both the `fluvio-app` and `fluvio-sys` charts.
    ///
    /// This option is mutually exclusive from [`remote_chart`]; if both are used,
    /// the latest one defined is the one that's used.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .local_chart("./k8-util/helm")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`remote_chart`]: ./struct.ClusterInstallerBuilder#method.remote_chart
    pub fn local_chart<S: Into<PathBuf>>(&mut self, local_chart_location: S) -> &mut Self {
        let user_chart_location = UserChartLocation::Local(local_chart_location.into());
        debug!(?user_chart_location, "setting local chart");
        self.chart_location(user_chart_location);
        self
    }

    /// Sets a remote helm chart location to search for Fluvio charts.
    ///
    /// This is the default case, with the default location being `https://charts.fluvio.io`,
    /// where official Fluvio helm charts are located. Remote helm charts are expected
    /// to be a valid URL.
    ///
    /// This option is mutually exclusive from [`local_chart`]; if both are used,
    /// the latest one defined is the one that's used.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .remote_chart("https://charts.fluvio.io")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`local_chart`]: ./struct.ClusterInstallerBuilder#method.local_chart
    pub fn remote_chart<S: Into<String>>(&mut self, remote_chart_location: S) -> &mut Self {
        self.chart_location(UserChartLocation::Remote(remote_chart_location.into()));
        self
    }

    /// Sets the TLS Policy that the client and server will use to communicate.
    ///
    /// By default, these are set to `TlsPolicy::Disabled`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterConfigBuilder, ClusterError};
    /// # fn example(builder: &mut ClusterConfigBuilder) -> Result<(), ClusterError> {
    /// use std::path::PathBuf;
    /// use fluvio::config::TlsPaths;
    /// use semver::Version;
    ///
    /// let cert_path = PathBuf::from("/tmp/certs");
    /// let client = TlsPaths {
    ///     domain: "fluvio.io".to_string(),
    ///     ca_cert: cert_path.join("ca.crt"),
    ///     cert: cert_path.join("client.crt"),
    ///     key: cert_path.join("client.key"),
    /// };
    /// let server = TlsPaths {
    ///     domain: "fluvio.io".to_string(),
    ///     ca_cert: cert_path.join("ca.crt"),
    ///     cert: cert_path.join("server.crt"),
    ///     key: cert_path.join("server.key"),
    /// };
    ///
    /// let config = ClusterConfig::builder(Version::parse("0.7.0-alpha.1").unwrap())
    ///     .tls(client, server)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls<C: Into<TlsPolicy>, S: Into<TlsPolicy>>(
        &mut self,
        client: C,
        server: S,
    ) -> &mut Self {
        let client_policy = client.into();
        let server_policy = server.into();

        use TlsPolicy::*;
        use std::mem::discriminant;
        match (&client_policy, &server_policy) {
            // If the two policies do not have the same variant, they are probably incompatible
            _ if discriminant(&client_policy) != discriminant(&server_policy) => {
                warn!("Client TLS policy type is different than the Server TLS policy type!");
            }
            // If the client and server domains do not match, give a warning
            (Verified(client), Verified(server)) if client.domain() != server.domain() => {
                warn!(
                    client_domain = client.domain(),
                    server_domain = server.domain(),
                    "Client TLS config has a different domain than the Server TLS config!"
                );
            }
            _ => (),
        }

        self.client_tls_policy = Some(client_policy);
        self.server_tls_policy = Some(server_policy);
        self
    }

    /// A builder helper for conditionally setting options
    ///
    /// This is useful for maintaining a builder call chain even when you
    /// only want to apply some options conditionally based on a boolean value.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, ClusterConfig};
    /// # fn example() -> Result<(), ClusterError> {
    /// use semver::Version;
    /// let custom_namespace = false;
    /// let config = ClusterConfig::builder(Version::parse("0.7.0-alpha.1").unwrap())
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

/// Allows installing Fluvio on a Kubernetes cluster
///
/// Fluvio's Kubernetes components are distributed as [Helm Charts],
/// which allow them to be easily installed on any Kubernetes
/// cluster. A `ClusterInstaller` takes care of installing all of
/// the pieces in the right order, with sane defaults.
///
/// If you want to try out Fluvio on Kubernetes, you can use [Minikube]
/// as an installation target. This is the default target that the
/// `ClusterInstaller` uses, so it doesn't require any complex setup.
///
/// # Example
///
/// ```
/// # use fluvio_cluster::{ClusterInstaller, ClusterConfig, ClusterError};
/// # async fn example() -> Result<(), ClusterError> {
/// use semver::Version;
/// let config = ClusterConfig::builder(Version::parse("0.7.0-alpha.1").unwrap()).build()?;
/// let installer = ClusterInstaller::from_config(config)?;
/// let _status = installer.install_fluvio().await?;
/// # Ok(())
/// # }
/// ```
///
/// [Helm Charts]: https://helm.sh/
/// [Minikube]: https://kubernetes.io/docs/tasks/tools/install-minikube/
#[derive(Debug)]
pub struct ClusterInstaller {
    /// Configuration options for this installation
    config: ClusterConfig,
    /// Shared Kubernetes client for install
    kube_client: K8Client,
    /// Helm client for performing installs
    helm_client: HelmClient,
}

impl ClusterInstaller {
    /// Creates a `ClusterInstaller` from a `ClusterConfig`
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterConfig, ClusterError, ClusterInstaller};
    /// # fn example(config: ClusterConfig) -> Result<(), ClusterError> {
    /// let installer = ClusterInstaller::from_config(config)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_config(config: ClusterConfig) -> Result<Self, ClusterError> {
        Ok(Self {
            config,
            kube_client: K8Client::default().map_err(K8InstallError::K8ClientError)?,
            helm_client: HelmClient::new().map_err(K8InstallError::HelmError)?,
        })
    }

    /// Checks if all of the prerequisites for installing Fluvio are met
    ///
    /// This will attempt to automatically fix any missing prerequisites,
    /// depending on the installer configuration. See the following options
    /// for more details:
    ///
    /// - [`system_chart`]
    /// - [`update_context`]
    ///
    /// [`system_chart`]: ./struct.ClusterInstaller.html#method.system_chart
    /// [`update_context`]: ./struct.ClusterInstaller.html#method.update_context
    #[instrument(skip(self))]
    pub async fn setup(&self) -> CheckResults {
        let mut sys_config: ChartConfig = ChartConfig::sys_builder()
            .version(self.config.chart_version.clone())
            .namespace(&self.config.namespace)
            .build()
            .unwrap();

        if let Some(location) = &self.config.chart_location {
            sys_config.location = location.to_owned().into();
        }

        let mut checker = ClusterChecker::empty()
            .with_k8_checks()
            .with_check(SysChartCheck::new(sys_config));

        if !self.config.upgrade {
            checker = checker.with_check(AlreadyInstalled);
        }

        if self.config.render_checks {
            let mut progress = checker.run_and_fix_with_progress();
            render_check_progress(&mut progress).await
        } else {
            checker.run_wait_and_fix().await
        }
    }

    /// Installs Fluvio according to the installer's configuration
    ///
    /// Returns the external address of the new cluster's SC
    #[instrument(
        skip(self),
        fields(namespace = &*self.config.namespace),
    )]
    pub async fn install_fluvio(&self) -> Result<StartStatus, ClusterError> {
        let checks = match self.config.skip_checks {
            true => None,
            // Check if env is ready for install and tries to fix anything it can
            false => {
                let check_results = self.setup().await;
                if check_results.iter().any(|it| it.is_err()) {
                    return Err(K8InstallError::PrecheckErrored(check_results).into());
                }

                let statuses: CheckStatuses =
                    check_results.into_iter().filter_map(|it| it.ok()).collect();

                let mut any_failed = false;
                for status in &statuses {
                    match status {
                        // If Fluvio is already installed, return the SC's address
                        CheckStatus::Fail(CheckFailed::AlreadyInstalled) => {
                            debug!("Fluvio is already installed. Getting SC address");
                            let (address, port) =
                                self.wait_for_sc_service(&self.config.namespace).await?;
                            return Ok(StartStatus {
                                address,
                                port,
                                checks: Some(statuses),
                            });
                        }
                        CheckStatus::Fail(_) => any_failed = true,
                        _ => (),
                    }
                }

                // If any of the pre-checks was a straight-up failure, install should fail
                if any_failed {
                    return Err(K8InstallError::FailedPrecheck(statuses).into());
                }
                Some(statuses)
            }
        };

        if let Some(proxy) = &self.config.proxy_addr {
            println!("Using proxy addr: {}", proxy);
        }
        self.install_app().await?;
        let namespace = &self.config.namespace;
        let (address, port) = self
            .wait_for_sc_service(namespace)
            .await
            .map_err(|_| K8InstallError::UnableToDetectService)?;
        println!("Fluvio SC is up at: {}", address);

        if self.config.save_profile {
            self.update_profile(address.clone())?;
        }

        let cluster =
            FluvioConfig::new(address.clone()).with_tls(self.config.client_tls_policy.clone());

        if self.config.spu_replicas > 0 && !self.config.upgrade {
            debug!("waiting for SC to spin up before attemp to spin up spu");
            // Wait a little bit for the SC to spin up
            sleep(Duration::from_millis(2000)).await;

            // Create a managed SPU cluster
            self.create_managed_spu_group(&cluster).await?;
        }

        // When upgrading, wait for platform version to match new version
        println!(
            "Waiting up to {} seconds for Fluvio cluster version check...",
            *MAX_SC_VERSION_LOOP * 2
        );
        self.wait_for_fluvio_version(&cluster).await?;

        // Wait for the SPU cluster to spin up
        if !self.config.skip_spu_liveness_check {
            self.wait_for_spu(namespace).await?;
        }

        Ok(StartStatus {
            address,
            port,
            checks,
        })
    }

    /// Install Fluvio Core chart on the configured cluster
    #[instrument(skip(self))]
    async fn install_app(&self) -> Result<(), K8InstallError> {
        debug!(
            "Installing fluvio with the following configuration: {:#?}",
            &self.config
        );

        // If configured with TLS, copy certs to server
        if let TlsPolicy::Verified(tls) = &self.config.server_tls_policy {
            self.upload_tls_secrets(tls)?;
        }

        // Specify common installation settings to pass to helm
        let mut install_settings: Vec<(_, Cow<str>)> =
            vec![("image.registry", Cow::Borrowed(&self.config.image_registry))];

        if let Some(tag) = &self.config.image_tag {
            install_settings.push(("image.tag", Cow::Borrowed(tag)))
        }

        let mut chart_values = self.config.chart_values.clone();

        // NodePort services need to provide SPU with an external address
        // We're going to provide it via annotation on the SPU's K8 service

        let _temp_files = if self.config.service_type == "NodePort" {
            // We're going to write the annotation to a temp file so Helm can use it
            // This is a workaround. More on this later in the function.
            let (np_addr_fd, np_conf_path) = NamedTempFile::new()?.into_parts();
            chart_values.push(np_conf_path.to_path_buf());

            debug!("Using NodePort service type");

            let external_addr = if let Some(addr) = &self.config.proxy_addr {
                debug!(?addr, "use proxying");
                addr.to_owned()
            } else {
                debug!("Getting external IP from K8s node");
                let kube_client = &self.kube_client;

                debug!("Trying to query for Nodes");

                let nodes = kube_client.retrieve_items::<NodeSpec, _>("").await?;

                debug!("Results from Node query: {:#?}", &nodes);

                let mut node_addr: Vec<NodeAddress> = Vec::new();
                for n in nodes.items.into_iter().map(|x| x.status.addresses) {
                    node_addr.extend(n)
                }

                debug!("Node Addresses: {:#?}", node_addr);

                node_addr
                    .into_iter()
                    .find(|a| a.r#type == "InternalIP")
                    .ok_or_else(|| K8InstallError::Other("No nodes with InternalIP set".into()))?
                    .address
            };

            // Set this annotation w/ the external address by overriding this Helm chart value:
            let mut ingress_address = BTreeMap::new();
            ingress_address.insert("fluvio.io/ingress-address", external_addr);

            let mut service_annotation = BTreeMap::new();
            service_annotation.insert("serviceAnnotations", ingress_address);

            let mut helm_lb_config = BTreeMap::new();
            helm_lb_config.insert("loadBalancer", service_annotation);

            debug!(?helm_lb_config, "helm_lb_config");

            serde_yaml::to_writer(&np_addr_fd, &helm_lb_config)
                .map_err(|err| K8InstallError::Other(err.to_string()))?;
            Some((np_addr_fd, np_conf_path))
        } else {
            None
        };

        // If TLS is enabled, set it as a helm variable
        if let TlsPolicy::Anonymous | TlsPolicy::Verified(_) = self.config.server_tls_policy {
            install_settings.push(("tls", Cow::Borrowed("true")));
        }

        // If RUST_LOG is defined, pass it to SC
        if let Some(log) = &self.config.rust_log {
            install_settings.push(("scLog", Cow::Borrowed(log)));
        }

        if let Some(authorization_config_map) = &self.config.authorization_config_map {
            install_settings.push((
                "authorizationConfigMap",
                Cow::Borrowed(authorization_config_map),
            ));
        }

        let install_settings: Vec<(String, String)> = install_settings
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_string()))
            .collect();

        debug!("Using helm install settings: {:#?}", &install_settings);

        println!("installing fluvio chart");

        let mut config = ChartConfig::app_builder()
            .namespace(&self.config.namespace)
            .version(self.config.chart_version.clone())
            .string_values(install_settings)
            .values(chart_values)
            .build()?;

        if let Some(location) = &self.config.chart_location {
            debug!(user_location=?location,"overriding with user chart location");
            config.location = location.to_owned().into();
        }

        let installer = ChartInstaller::from_config(config)?;
        installer.process(self.config.upgrade)?;

        info!("Fluvio app chart has been installed");
        Ok(())
    }

    /// Uploads TLS secrets to Kubernetes
    fn upload_tls_secrets(&self, tls: &TlsConfig) -> Result<(), K8InstallError> {
        let paths: Cow<TlsPaths> = match tls {
            TlsConfig::Files(paths) => Cow::Borrowed(paths),
            TlsConfig::Inline(certs) => Cow::Owned(certs.try_into_temp_files()?),
        };
        self.upload_tls_secrets_from_files(paths.as_ref())?;
        Ok(())
    }

    /// Looks up the external address of a Fluvio SC instance in the given namespace
    #[instrument(skip(self, ns))]
    async fn discover_sc_address(&self, ns: &str) -> Result<Option<(String, u16)>, K8InstallError> {
        use tokio::select;
        use futures_lite::stream::StreamExt;

        use fluvio_future::timer::sleep;
        use k8_types::K8Watch;

        let mut service_stream = self
            .kube_client
            .watch_stream_now::<ServiceSpec>(ns.to_string());

        let mut timer = sleep(Duration::from_secs(*MAX_SC_SERVICE_WAIT));
        loop {
            select! {
                _ = &mut timer => {
                    debug!(timer = *MAX_SC_SERVICE_WAIT,"timer expired");
                    return Ok(None)
                },
                service_next = service_stream.next() => {
                    if let Some(service_watches) = service_next {

                        for service_watch in service_watches? {
                            let service_value = match service_watch? {
                                K8Watch::ADDED(svc) => Some(svc),
                                K8Watch::MODIFIED(svc) => Some(svc),
                                K8Watch::DELETED(_) => None
                            };

                            if let Some(service) = service_value {

                                if service.metadata.name == FLUVIO_SC_SERVICE {
                                    debug!(service = ?service,"found sc service");

                                    let target_port =  service.spec
                                        .ports
                                        .iter()
                                        .filter_map(|port| {
                                            match port.target_port {
                                                Some(TargetPort::Number(value)) => Some(value),
                                                Some(TargetPort::Name(_)) => None,
                                                None => None
                                            }
                                        })
                                        .next()
                                        .ok_or_else(|| K8InstallError::Other("target port should be there".into()))?;

                                    let node_port =  service.spec
                                        .ports
                                        .iter()
                                        .filter_map(|port| port.node_port)
                                        .next();


                                    if self.config.use_cluster_ip  {
                                        return Ok(Some((format!("{}:{}",service.spec.cluster_ip,target_port),target_port)))
                                    };

                                    let k8_load_balancer_type = service.spec.r#type.ok_or_else(|| K8InstallError::Other("Load Balancer Type".into()))?;

                                    match k8_load_balancer_type {
                                        LoadBalancerType::ClusterIP => {
                                            return Ok(Some((format!("{}:{}",service.spec.cluster_ip,target_port),target_port)))
                                        },
                                        LoadBalancerType::NodePort => {
                                            let node_port = node_port.ok_or_else(|| K8InstallError::Other("Expecting a NodePort port".into()))?;

                                            let extern_addr = if let Some(addr) = &self.config.proxy_addr {
                                                debug!(?addr,"using proxy");
                                                addr.to_owned()
                                            } else {

                                                debug!("k8 node query");
                                                let nodes = self.kube_client.retrieve_items::<NodeSpec, _>(ns).await?;
                                                debug!("Output from k8 node query: {:#?}", &nodes);

                                                let mut node_addr : Vec<NodeAddress> = Vec::new();
                                                for n in nodes.items.into_iter().map(|x| x.status.addresses ) {
                                                    node_addr.extend(n)
                                                }

                                                // Return the first node with type "InternalIP"
                                                node_addr.into_iter().find(|a| a.r#type == "InternalIP")
                                                    .ok_or_else(|| K8InstallError::Other("No nodes with InternalIP set".into()))?.address
                                            };

                                            return Ok(Some((format!("{}:{}",extern_addr,node_port),node_port)))

                                        },
                                        LoadBalancerType::LoadBalancer => {
                                            let ingress_addr = service
                                                .status
                                                .load_balancer
                                                .ingress
                                                .iter()
                                                .find(|_| true)
                                                .and_then(|ingress| ingress.host_or_ip().to_owned());

                                            if let Some(sock_addr) = ingress_addr.map(|addr| {format!("{}:{}", addr, target_port)}) {
                                                    debug!(%sock_addr,"found lb address");
                                                    return Ok(Some((sock_addr,target_port)))
                                            }
                                        },
                                        LoadBalancerType::ExternalName => {
                                            unimplemented!("ExternalName Load Balancer support not implemented");
                                        },

                                    }

                                }
                            }
                        }
                    } else {
                        debug!("service stream ended");
                        return Ok(None)
                    }
                }
            }
        }
    }

    /// Wait until the platform version of the cluster matches the chart version here
    #[instrument(skip(self))]
    async fn wait_for_fluvio_version(&self, config: &FluvioConfig) -> Result<(), K8InstallError> {
        for attempt in 0..*MAX_SC_VERSION_LOOP {
            let fluvio = match Fluvio::connect_with_config(config).await {
                Ok(fluvio) => fluvio,
                Err(_) => {
                    sleep(Duration::from_millis(2_000)).await;
                    continue;
                }
            };

            // The major.minor.patch versions should match after upgrade
            let platform_version = fluvio.platform_version();
            let compatible = versions_compatible(
                platform_version.clone(),
                self.config.platform_version.clone(),
            );

            if compatible {
                // Success
                break;
            }
            debug!(
                platform = %platform_version,
                platform_version = %self.config.platform_version,
                "Existing platform version is different than platform version",
            );
            if attempt >= *MAX_SC_VERSION_LOOP - 1 {
                return Err(K8InstallError::FailedPlatformVersion(
                    self.config.platform_version.to_string(),
                ));
            }
            sleep(Duration::from_millis(2_000)).await;
        }

        Ok(())
    }

    /// Wait until the Fluvio SC public service appears in Kubernetes
    /// return address and port
    #[instrument(skip(self, ns))]
    async fn wait_for_sc_service(&self, ns: &str) -> Result<(String, u16), K8InstallError> {
        debug!("waiting for SC service");
        if let Some((sock_addr, port)) = self.discover_sc_address(ns).await? {
            debug!(%sock_addr, "found SC service addr");
            self.wait_for_sc_port_check(&sock_addr).await?;
            Ok((sock_addr, port))
        } else {
            Err(K8InstallError::SCServiceTimeout)
        }
    }

    /// Wait until the Fluvio SC public service appears in Kubernetes
    async fn wait_for_sc_port_check(&self, sock_addr_str: &str) -> Result<(), K8InstallError> {
        for i in 0..*MAX_SC_NETWORK_LOOP {
            if self.config.proxy_addr.is_none() {
                debug!("resolving socket addr: {}", sock_addr_str);
                let sock_addr = self.wait_for_sc_dns(sock_addr_str).await?;
                if TcpStream::connect(&*sock_addr).await.is_ok() {
                    info!(sock_addr = %sock_addr_str, "finished SC port check");
                    return Ok(());
                }
            } else {
                debug!("trying to connect to proxy: {}", sock_addr_str);
                if TcpStream::connect(&*sock_addr_str).await.is_ok() {
                    info!(sock_addr = %sock_addr_str, "finished SC port check");
                    return Ok(());
                }
            }
            info!(
                attempt = i,
                "sc port closed, sleeping for {} ms", NETWORK_SLEEP_MS
            );
            sleep(Duration::from_millis(NETWORK_SLEEP_MS)).await;
        }
        error!(sock_addr = %sock_addr_str, "timeout for SC port check");
        Err(K8InstallError::SCPortCheckTimeout)
    }

    /// Wait until the Fluvio SC public service appears in Kubernetes
    async fn wait_for_sc_dns(
        &self,
        sock_addr_string: &str,
    ) -> Result<Vec<SocketAddr>, K8InstallError> {
        debug!("waiting for SC dns resolution: {}", sock_addr_string);
        for i in 0..*MAX_SC_NETWORK_LOOP {
            match resolve(sock_addr_string).await {
                Ok(sock_addr) => {
                    debug!("finished SC dns resolution: {}", sock_addr_string);
                    return Ok(sock_addr);
                }
                Err(err) => {
                    info!(
                        attempt = i,
                        "SC dns resoultion failed {}, sleeping for {} ms", err, NETWORK_SLEEP_MS
                    );
                    sleep(Duration::from_millis(NETWORK_SLEEP_MS)).await;
                }
            }
        }

        error!("timedout sc dns: {}", sock_addr_string);
        Err(K8InstallError::SCDNSTimeout)
    }

    /// Wait until all SPUs are ready and have ingress
    #[instrument(skip(self, ns))]
    async fn wait_for_spu(&self, ns: &str) -> Result<bool, K8InstallError> {
        debug!("waiting for SPU with: {} loop", *MAX_SC_NETWORK_LOOP);
        for i in 0..*MAX_SC_NETWORK_LOOP {
            debug!("retrieving spu specs");
            let items = self.kube_client.retrieve_items::<SpuSpec, _>(ns).await?;
            let spu_count = items.items.len();

            // We want to know what kind of load balancing we're using, for liveness checks
            let service = self
                .kube_client
                .retrieve_items::<ServiceSpec, _>(ns)
                .await?;

            let sc_public_svc = service
                .items
                .into_iter()
                .find(|sc_lb| sc_lb.metadata.name == "fluvio-sc-public");

            let lb_type = if let Some(k8svc) = sc_public_svc {
                k8svc.spec.r#type
            } else {
                Some(LoadBalancerType::LoadBalancer)
            };

            // Check that all items have ingress
            let ready_spu = items
                .items
                .iter()
                .filter(|spu_obj| {
                    match lb_type {
                        Some(LoadBalancerType::NodePort) => spu_obj.status.is_online(),
                        _ => {
                            // if cluster ip is used then we skip checking ingress
                            (self.config.use_cluster_ip
                                || !spu_obj.spec.public_endpoint.ingress.is_empty())
                                && spu_obj.status.is_online()
                        }
                    }
                })
                .count();

            if self.config.spu_replicas as usize == ready_spu {
                println!("All SPUs({}) are ready", ready_spu);
                return Ok(true);
            } else {
                debug!(
                    total_expected_spu = spu_count,
                    ready_spu,
                    attempt = i,
                    "Not all SPUs are ready. Waiting",
                );
                println!(
                    "{} of {} spu are ready, sleeping 10 seconds...",
                    ready_spu, self.config.spu_replicas,
                );
                sleep(Duration::from_secs(10)).await;
            }
        }

        Err(K8InstallError::SPUTimeout)
    }

    /// Install server-side TLS by uploading secrets to kubernetes
    #[instrument(skip(self, paths))]
    fn upload_tls_secrets_from_files(&self, paths: &TlsPaths) -> Result<(), K8InstallError> {
        let ca_cert = paths
            .ca_cert
            .to_str()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidInput, "ca_cert must be a valid path"))?;
        let server_cert = paths.cert.to_str().ok_or_else(|| {
            IoError::new(ErrorKind::InvalidInput, "server_cert must be a valid path")
        })?;
        let server_key = paths.key.to_str().ok_or_else(|| {
            IoError::new(ErrorKind::InvalidInput, "server_key must be a valid path")
        })?;
        debug!("Using TLS from paths: {:?}", paths);

        // Try uninstalling secrets first to prevent duplication error
        Command::new("kubectl")
            .args(&["delete", "secret", "fluvio-ca", "--ignore-not-found=true"])
            .args(&["--namespace", &self.config.namespace])
            .inherit()
            .result()?;

        Command::new("kubectl")
            .args(&["delete", "secret", "fluvio-tls", "--ignore-not-found=true"])
            .args(&["--namespace", &self.config.namespace])
            .inherit()
            .result()?;

        Command::new("kubectl")
            .args(&["create", "secret", "generic", "fluvio-ca"])
            .args(&["--from-file", ca_cert])
            .args(&["--namespace", &self.config.namespace])
            .inherit()
            .result()?;

        Command::new("kubectl")
            .args(&["create", "secret", "tls", "fluvio-tls"])
            .args(&["--cert", server_cert])
            .args(&["--key", server_key])
            .args(&["--namespace", &self.config.namespace])
            .inherit()
            .result()?;

        Ok(())
    }

    /// Updates the Fluvio configuration with the newly installed cluster info.
    fn update_profile(&self, external_addr: String) -> Result<(), K8InstallError> {
        debug!("updating profile for: {}", external_addr);
        let mut config_file = ConfigFile::load_default_or_new()?;
        let config = config_file.mut_config();

        let profile_name = self.compute_profile_name()?;

        match config.cluster_mut(&profile_name) {
            Some(cluster) => {
                cluster.endpoint = external_addr;
                cluster.tls = self.config.client_tls_policy.clone();
            }
            None => {
                let mut local_cluster = FluvioConfig::new(external_addr);
                local_cluster.tls = self.config.client_tls_policy.clone();
                config.add_cluster(local_cluster, profile_name.clone());
            }
        }

        match config.profile_mut(&profile_name) {
            Some(profile) => {
                profile.set_cluster(profile_name.clone());
            }
            None => {
                let profile = Profile::new(profile_name.clone());
                config.add_profile(profile, profile_name.clone());
            }
        };

        config.set_current_profile(&profile_name);
        config_file.save()?;
        Ok(())
    }

    /// Determines a profile name from the name of the active Kubernetes context
    fn compute_profile_name(&self) -> Result<String, K8InstallError> {
        let k8_config = K8Config::load()?;

        let kc_config = match k8_config {
            K8Config::Pod(_) => {
                return Err(K8InstallError::Other(
                    "Pod config is not valid here".to_owned(),
                ));
            }
            K8Config::KubeConfig(config) => config,
        };

        kc_config
            .config
            .current_context()
            .ok_or_else(|| K8InstallError::Other("No context fount".to_owned()))
            .map(|ctx| ctx.name.to_owned())
    }

    /// Provisions a SPU group for the given cluster according to internal config
    #[instrument(
    skip(self, cluster),
    fields(cluster_endpoint = & * cluster.endpoint)
    )]
    async fn create_managed_spu_group(&self, cluster: &FluvioConfig) -> Result<(), K8InstallError> {
        debug!("trying to create managed spu: {:#?}", cluster);
        let name = self.config.group_name.clone();
        let fluvio = Fluvio::connect_with_config(cluster).await?;
        let admin = fluvio.admin().await;

        let spu_spec = SpuGroupSpec {
            replicas: self.config.spu_replicas,
            min_id: 0,
            ..SpuGroupSpec::default()
        };

        admin.create(name, false, spu_spec).await?;
        Ok(())
    }
}

fn versions_compatible(a: Version, b: Version) -> bool {
    Version::new(a.major, a.minor, a.patch) == Version::new(b.major, b.minor, b.patch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_config() {
        let config: ClusterConfig =
            ClusterConfig::builder(semver::Version::parse("0.7.0-alpha.1").unwrap())
                .build()
                .expect("should succeed with required config options");
        assert_eq!(
            config.platform_version,
            semver::Version::parse("0.7.0-alpha.1").unwrap()
        )
    }

    #[test]
    fn test_compatible_prerelease() {
        let a = Version::parse("0.8.0").unwrap();
        let b = Version::parse("0.8.0-alpha.4").unwrap();
        assert!(versions_compatible(a, b));
    }

    #[test]
    fn test_compatible_commits() {
        let a = Version::parse("0.8.0-abcdef").unwrap();
        let b = Version::parse("0.8.0-fedcba").unwrap();
        assert!(versions_compatible(a, b));
    }

    #[test]
    fn test_compatible_pre_and_build() {
        let a = Version::parse("0.8.0-alpha.2").unwrap();
        let b = Version::parse("0.8.0-abcdef+abcdef").unwrap();
        assert!(versions_compatible(a, b));
    }
}
