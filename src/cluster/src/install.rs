use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::borrow::Cow;
use std::process::Command;
use std::time::Duration;
use std::net::IpAddr;
use std::str::FromStr;

use tracing::{info, warn, debug, trace, instrument};
use fluvio::{Fluvio, FluvioConfig};
use fluvio::metadata::spg::SpuGroupSpec;
use fluvio::metadata::spu::SpuSpec;
use fluvio::config::{TlsPolicy, TlsConfig, TlsPaths, ConfigFile, Profile};
use flv_util::cmd::CommandExt;
use fluvio_future::timer::sleep;
use k8_client::{K8Client, ClientError as K8ClientError};
use k8_config::{K8Config};
use k8_client::metadata::MetadataClient;
use k8_obj_core::service::ServiceSpec;
use k8_obj_metadata::InputObjectMeta;
use semver::Version;

use crate::ClusterError;
use crate::helm::{HelmClient, Chart, InstalledChart};
use crate::check::get_cluster_server_host;

const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_REGISTRY: &str = "infinyon";
const DEFAULT_CHART_SYS_REPO: &str = "fluvio-sys";
const DEFAULT_CHART_SYS_NAME: &str = "fluvio/fluvio-sys";
const DEFAULT_CHART_APP_REPO: &str = "fluvio";
const DEFAULT_CHART_APP_NAME: &str = "fluvio/fluvio-app";
const DEFAULT_CHART_REMOTE: &str = "https://infinyon.github.io";
const DEFAULT_GROUP_NAME: &str = "main";
const DEFAULT_CLOUD_NAME: &str = "minikube";
const DEFAULT_HELM_VERSION: &str = "3.2.0";

/// Distinguishes between a Local and Remote helm chart
#[derive(Debug)]
enum ChartLocation {
    /// Local charts must be located at a valid filesystem path.
    Local(PathBuf),
    /// Remote charts will be located at a URL such as `https://...`
    Remote(String),
}

/// A builder for cluster installation options
#[derive(Debug)]
pub struct ClusterInstallerBuilder {
    /// The namespace to install under
    namespace: String,
    /// The image tag to use for Fluvio install
    image_tag: Option<String>,
    /// The docker registry to use
    image_registry: String,
    /// The name of the Fluvio helm chart to install
    chart_name: String,
    /// A specific version of the Fluvio helm chart to install
    chart_version: String,
    /// The location to find the helm chart
    chart_location: ChartLocation,
    /// The name of the SPU group to create
    group_name: String,
    /// The name of the Fluvio cloud
    cloud: String,
    /// Whether to save an update to the Fluvio profile
    save_profile: bool,
    /// How much storage to allocate on SPUs
    spu_spec: SpuGroupSpec,
    /// The logging settings to set in the cluster
    rust_log: Option<String>,
    /// The TLS policy for the SC and SPU servers
    server_tls_policy: TlsPolicy,
    /// The TLS policy for the client
    client_tls_policy: TlsPolicy,
}

impl ClusterInstallerBuilder {
    /// Creates a [`ClusterInstaller`] with the current configuration.
    ///
    /// This may fail if there is a problem conencting to Kubernetes or
    /// finding the `helm` executable on the local system.
    ///
    /// # Example
    ///
    /// The simplest flow to create a `ClusterInstaller` looks like the
    /// following:
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .build()
    ///     .expect("should create ClusterInstaller");
    /// ```
    ///
    /// [`ClusterInstaller`]: ./struct.ClusterInstaller.html
    pub fn build(self) -> Result<ClusterInstaller, ClusterError> {
        Ok(ClusterInstaller {
            config: self,
            kube_client: K8Client::default()?,
            helm_client: HelmClient::new()?,
        })
    }

    /// Sets the Kubernetes namespace to install Fluvio into.
    ///
    /// The default namespace is "default".
    pub fn with_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.namespace = namespace.into();
        self
    }

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
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_image_tag("0.6.0")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_image_tag<S: Into<String>>(mut self, image_tag: S) -> Self {
        self.image_tag = Some(image_tag.into());
        self
    }

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
    /// > **NOTE**: See [`with_image_tag`] to see how to specify the `0.1.0` shown here.
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_image_registry("localhost:5000/infinyon")
    ///     .build()
    ///     .unwrap();
    /// ```
    ///
    /// Then, when you use `installer.install_fluvio()`, it will pull the images
    /// from your local docker registry.
    ///
    /// [`with_image_tag`]: ./struct.ClusterInstaller.html#method.with_image_tag
    pub fn with_image_registry<S: Into<String>>(mut self, image_registry: S) -> Self {
        self.image_registry = image_registry.into();
        self
    }

    /// Sets a specific version of the Fluvio helm chart to install.
    ///
    /// When working with published Fluvio images, the chart version will appear
    /// to be a [Semver] version, such as `0.6.0`.
    ///
    /// When developing for Fluvio, chart versions are named after the git hash
    /// of the revision a chart was built on.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_chart_version("0.6.0")
    ///     .build()
    ///     .unwrap();
    /// ```
    ///
    /// [Semver]: https://docs.rs/semver/0.10.0/semver/
    pub fn with_chart_version<S: Into<String>>(mut self, chart_version: S) -> Self {
        self.chart_version = chart_version.into();
        self
    }

    /// Sets a local helm chart location to search for Fluvio charts.
    ///
    /// This is often desirable when developing for Fluvio locally and making
    /// edits to the chart. When using this option, the argument is expected to be
    /// a local filesystem path.
    ///
    /// This option is mutually exclusive from [`with_remote_chart`]; if both are used,
    /// the latest one defined is the one that's used.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_local_chart("./k8-util/helm/fluvio-app")
    ///     .build()
    ///     .unwrap();
    /// ```
    ///
    /// [`with_remote_chart`]: ./struct.ClusterInstallerBuilder#method.with_remote_chart
    pub fn with_local_chart<S: Into<PathBuf>>(mut self, local_chart_location: S) -> Self {
        self.chart_location = ChartLocation::Local(local_chart_location.into());
        self
    }

    /// Sets a remote helm chart location to search for Fluvio charts.
    ///
    /// This is the default case, with the default location being `https://infinyon.github.io`,
    /// where official Fluvio helm charts are located. Remote helm charts are expected
    /// to be a valid URL.
    ///
    /// This option is mutually exclusive from [`with_local_chart`]; if both are used,
    /// the latest one defined is the one that's used.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_remote_chart("https://infinyon.github.io")
    ///     .build()
    ///     .unwrap();
    /// ```
    ///
    /// [`with_local_chart`]: ./struct.ClusterInstallerBuilder#method.with_local_chart
    pub fn with_remote_chart<S: Into<String>>(mut self, remote_chart_location: S) -> Self {
        self.chart_location = ChartLocation::Remote(remote_chart_location.into());
        self
    }

    /// Sets a custom SPU group name. The default is `main`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_group_name("orange")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_group_name<S: Into<String>>(mut self, group_name: S) -> Self {
        self.group_name = group_name.into();
        self
    }

    /// Whether to save a profile of this installation to `~/.fluvio/config`. Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_save_profile(true)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_save_profile(mut self, save_profile: bool) -> Self {
        self.save_profile = save_profile;
        self
    }

    /// Sets the number of SPU replicas that should be provisioned. Defaults to 1.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_spu_replicas(2)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_spu_replicas(mut self, spu_replicas: u16) -> Self {
        self.spu_spec.replicas = spu_replicas;
        self
    }

    /// Sets the [`RUST_LOG`] environment variable for the installation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_rust_log("debug")
    ///     .build()
    ///     .unwrap();
    /// ```
    ///
    /// [`RUST_LOG`]: https://docs.rs/tracing-subscriber/0.2.11/tracing_subscriber/filter/struct.EnvFilter.html
    pub fn with_rust_log<S: Into<String>>(mut self, rust_log: S) -> Self {
        self.rust_log = Some(rust_log.into());
        self
    }

    /// Sets the TLS Policy that the client and server will use to communicate.
    ///
    /// By default, these are set to `TlsPolicy::Disabled`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    /// use fluvio::config::TlsPaths;
    /// use fluvio_cluster::ClusterInstaller;
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
    /// let installer = ClusterInstaller::new()
    ///     .with_tls(client, server)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_tls<C: Into<TlsPolicy>, S: Into<TlsPolicy>>(
        mut self,
        client: C,
        server: S,
    ) -> Self {
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

        self.client_tls_policy = client_policy;
        self.server_tls_policy = server_policy;
        self
    }
}

/// Allows installing Fluvio on a Kubernetes cluster
///
/// Fluvio's Kubernetes components are distributed as [Helm Charts],
/// which allow them to be easily installed on any Kubernetes
/// cluster. A `ClusterInstaller` takes care of installing all of
/// the pieces in the right order, with sane defaults.
///
/// [Helm Charts]: https://helm.sh/
///
/// # Example
///
/// If you want to try out Fluvio on Kubernetes, you can use [Minikube]
/// as an installation target. This is the default target that the
/// `ClusterInstaller` uses, so it doesn't require any complex setup.
///
/// [Minikube]: https://kubernetes.io/docs/tasks/tools/install-minikube/
///
/// ```no_run
/// use fluvio_cluster::ClusterInstaller;
/// let installer = ClusterInstaller::new()
///     .build()
///     .expect("should initialize installer");
///
/// // Installing Fluvio is asynchronous, so you'll need an async runtime
/// let result = fluvio_future::task::run_block_on(async {
///     installer.install_fluvio().await
/// });
/// ```
#[derive(Debug)]
pub struct ClusterInstaller {
    /// Configuration options for this installation
    config: ClusterInstallerBuilder,
    /// Shared Kubernetes client for install
    kube_client: K8Client,
    /// Helm client for performing installs
    helm_client: HelmClient,
}

impl ClusterInstaller {
    /// Creates a default [`ClusterInstallerBuilder`] which can build a `ClusterInstaller`
    ///
    /// # Example
    ///
    /// The easiest way to build a `ClusterInstaller` is as follows:
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new().build().unwrap();
    /// ```
    ///
    /// Building a `ClusterInstaller` may fail if there is trouble connecting to
    /// Kubernetes or if the `helm` executable is not locally installed.
    ///
    /// You may also set custom installation options before calling [`.build()`].
    /// For example, if you wanted to specify a custom namespace and RUST_LOG,
    /// you could use [`with_namespace`] and [`with_rust_log`]. See
    /// [`ClusterInstallerBuilder`] for the full set of options.
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_namespace("my_namespace")
    ///     .with_rust_log("debug")
    ///     .build()
    ///     .expect("should build ClusterInstaller");
    /// ```
    ///
    /// [`ClusterInstallerBuilder`]: ./struct.ClusterInstallerBuilder.html
    /// [`.build()`]: ./struct.ClusterInstallerBuilder.html#build
    /// [`with_namespace`]: ./struct.ClusterInstallerBuilder.html#method.with_namespace
    /// [`with_rust_log`]: ./struct.ClusterInstallerBuilder.html#method.with_rust_log
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ClusterInstallerBuilder {
        let spu_spec = SpuGroupSpec {
            replicas: 1,
            min_id: 0,
            ..SpuGroupSpec::default()
        };

        ClusterInstallerBuilder {
            namespace: DEFAULT_NAMESPACE.to_string(),
            image_tag: None,
            image_registry: DEFAULT_REGISTRY.to_string(),
            chart_version: crate::VERSION.to_string(),
            chart_name: DEFAULT_CHART_APP_NAME.to_string(),
            chart_location: ChartLocation::Remote(DEFAULT_CHART_REMOTE.to_string()),
            group_name: DEFAULT_GROUP_NAME.to_string(),
            cloud: DEFAULT_CLOUD_NAME.to_string(),
            save_profile: false,
            spu_spec,
            rust_log: None,
            server_tls_policy: TlsPolicy::Disabled,
            client_tls_policy: TlsPolicy::Disabled,
        }
    }

    /// Get all the available versions of fluvio chart
    pub fn versions() -> Result<Vec<Chart>, ClusterError> {
        let helm_client = HelmClient::new()?;
        let versions = helm_client.versions(DEFAULT_CHART_APP_NAME)?;
        Ok(versions)
    }

    /// Get installed system chart
    pub fn sys_charts() -> Result<Vec<InstalledChart>, ClusterError> {
        let helm_client = HelmClient::new()?;
        let sys_charts = helm_client.get_installed_chart_by_name(DEFAULT_CHART_SYS_REPO)?;
        Ok(sys_charts)
    }

    /// Runs pre install checks
    ///  1. Check if compatible helm version is installed
    ///  2. Check if compatible sys charts are installed
    ///  3. Check if the K8 config hostname is not an IP address
    fn pre_install_check(&self) -> Result<(), ClusterError> {
        // check helm version
        let version_text_trimmed = self.helm_client.get_helm_version()?;
        if Version::parse(&version_text_trimmed) < Version::parse(DEFAULT_HELM_VERSION) {
            return Err(ClusterError::Other(format!(
                "Helm version {} is not compatible with fluvio platform, please install version >= {}",
                version_text_trimmed, DEFAULT_HELM_VERSION
            )));
        }

        // check installed system chart version
        let sys_charts = self
            .helm_client
            .get_installed_chart_by_name(DEFAULT_CHART_SYS_REPO)?;
        if sys_charts.is_empty() {
            return Err(ClusterError::Other(
                "Fluvio system chart is not installed, please install fluvio-sys first".to_string(),
            ));
        } else if sys_charts.len() > 1 {
            return Err(ClusterError::Other(
                "Multiple fluvio system charts found".to_string(),
            ));
        }

        // check k8 config hostname is not an IP address
        let k8_config = K8Config::load()?;
        match k8_config {
            K8Config::Pod(_) => {
                // ignore server check for pod
            }
            K8Config::KubeConfig(config) => {
                let server_host = match get_cluster_server_host(config) {
                    Ok(server) => server,
                    Err(e) => {
                        return Err(ClusterError::Other(format!(
                            "error fetching server from kube context {}",
                            e.to_string()
                        )))
                    }
                };
                if !server_host.trim().is_empty() {
                    if IpAddr::from_str(&server_host).is_ok() {
                        return Err(ClusterError::Other(
                            format!("Cluster in kube context cannot use IP address, please use minikube context: {}", server_host),
                        ));
                    };
                } else {
                    return Err(ClusterError::Other(
                        "Cluster in kubectl context cannot have empty hostname".to_owned(),
                    ));
                }
            }
        };
        Ok(())
    }

    /// Installs Fluvio according to the installer's configuration
    ///
    /// Returns the external address of the new cluster's SC
    #[instrument(
        skip(self),
        fields(namespace = &*self.config.namespace),
    )]
    pub async fn install_fluvio(&self) -> Result<String, ClusterError> {
        // perform pre install checks
        self.pre_install_check()?;
        self.install_app()?;

        let namespace = &self.config.namespace;
        let sc_address = match self.wait_for_sc_service(namespace).await {
            Ok(Some(addr)) => {
                info!(addr = &*addr, "Fluvio SC is up");
                addr
            }
            Ok(None) => {
                warn!("Timed out when waiting for SC service");
                return Err(ClusterError::Other(
                    "Timed out when waiting for SC service".to_string(),
                ));
            }
            Err(err) => {
                warn!("Unable to detect Fluvio service. If you're running on Minikube, make sure you have the tunnel up!");
                return Err(ClusterError::Other(format!(
                    "Unable to detect Fluvio service: {}",
                    err
                )));
            }
        };

        if self.config.save_profile {
            self.update_profile(sc_address.clone())?;
        }

        if self.config.spu_spec.replicas > 0 {
            // Wait a little bit for the SC to spin up
            sleep(Duration::from_millis(2000)).await;

            // Create a managed SPU cluster
            let cluster = FluvioConfig::new(sc_address.clone())
                .with_tls(self.config.client_tls_policy.clone());
            self.create_managed_spu_group(&cluster).await?;

            // Wait for the SPU cluster to spin up
            if !self.wait_for_spu(namespace).await? {
                warn!("SPU took too long to get ready");
                return Err(ClusterError::Other(
                    "SPU took too long to get ready".to_string(),
                ));
            }
        }

        Ok(sc_address)
    }

    /// Install the Fluvio System chart on the configured cluster
    // TODO: Try to make install_sys a subroutine of install_fluvio
    // TODO: by performing checks before installation.
    // TODO: Discussion at https://github.com/infinyon/fluvio/issues/235
    #[doc(hidden)]
    #[instrument(skip(self))]
    pub fn _install_sys(&self) -> Result<(), ClusterError> {
        let install_settings = &[("cloud", &*self.config.cloud)];
        match &self.config.chart_location {
            ChartLocation::Remote(chart_location) => {
                debug!(
                    chart_location = &**chart_location,
                    "Using remote helm chart:"
                );
                self.helm_client
                    .repo_add(DEFAULT_CHART_APP_REPO, chart_location)?;
                self.helm_client.repo_update()?;
                self.helm_client.install(
                    &self.config.namespace,
                    DEFAULT_CHART_SYS_REPO,
                    DEFAULT_CHART_SYS_NAME,
                    None,
                    install_settings,
                )?;
            }
            ChartLocation::Local(chart_location) => {
                let chart_location = chart_location.to_string_lossy();
                debug!(
                    chart_location = chart_location.as_ref(),
                    "Using local helm chart:"
                );
                self.helm_client.install(
                    &self.config.namespace,
                    DEFAULT_CHART_SYS_REPO,
                    chart_location.as_ref(),
                    None,
                    install_settings,
                )?;
            }
        }

        info!("Fluvio sys chart has been installed");
        Ok(())
    }

    /// Install Fluvio Core chart on the configured cluster
    #[instrument(skip(self))]
    fn install_app(&self) -> Result<(), ClusterError> {
        trace!(
            "Installing fluvio with the following configuration: {:#?}",
            &self.config
        );

        // If configured with TLS, copy certs to server
        if let TlsPolicy::Verified(tls) = &self.config.server_tls_policy {
            self.upload_tls_secrets(tls)?;
        }

        let fluvio_tag = self
            .config
            .image_tag
            .as_ref()
            .unwrap_or(&self.config.chart_version)
            .to_owned();

        // Specify common installation settings to pass to helm
        let mut install_settings: Vec<(_, &str)> = vec![
            ("image.registry", &self.config.image_registry),
            ("image.tag", &fluvio_tag),
            ("cloud", &self.config.cloud),
        ];

        // If TLS is enabled, set it as a helm variable
        if let TlsPolicy::Anonymous | TlsPolicy::Verified(_) = self.config.server_tls_policy {
            install_settings.push(("tls", "true"));
        }

        // If RUST_LOG is defined, pass it to SC
        if let Some(log) = &self.config.rust_log {
            install_settings.push(("scLog", log));
        }

        match &self.config.chart_location {
            // For remote, we add a repo pointing to the chart location.
            ChartLocation::Remote(chart_location) => {
                self.helm_client
                    .repo_add(DEFAULT_CHART_APP_REPO, chart_location)?;
                self.helm_client.repo_update()?;
                if !self
                    .helm_client
                    .chart_version_exists(&self.config.chart_name, &self.config.chart_version)?
                {
                    return Err(ClusterError::Other(format!(
                        "{}:{} not found in helm repo",
                        &self.config.chart_name, &self.config.chart_version,
                    )));
                }
                debug!(
                    chart_location = &**chart_location,
                    "Using remote helm chart:"
                );
                self.helm_client.install(
                    &self.config.namespace,
                    DEFAULT_CHART_APP_REPO,
                    &self.config.chart_name,
                    Some(&self.config.chart_version),
                    &install_settings,
                )?;
            }
            // For local, we do not use a repo but install from the chart location directly.
            ChartLocation::Local(chart_location) => {
                let chart_location = chart_location.to_string_lossy();
                debug!(
                    chart_location = chart_location.as_ref(),
                    "Using local helm chart:"
                );
                self.helm_client.install(
                    &self.config.namespace,
                    DEFAULT_CHART_APP_REPO,
                    chart_location.as_ref(),
                    Some(&self.config.chart_version),
                    &install_settings,
                )?;
            }
        }

        info!("Fluvio app chart has been installed");
        Ok(())
    }

    /// Uploads TLS secrets to Kubernetes
    fn upload_tls_secrets(&self, tls: &TlsConfig) -> Result<(), IoError> {
        let paths: Cow<TlsPaths> = match tls {
            TlsConfig::Files(paths) => Cow::Borrowed(paths),
            TlsConfig::Inline(certs) => Cow::Owned(certs.try_into_temp_files()?),
        };
        self.upload_tls_secrets_from_files(paths.as_ref())?;
        Ok(())
    }

    /// Looks up the external address of a Fluvio SC instance in the given namespace
    #[instrument(skip(self, ns))]
    async fn discover_sc_address(&self, ns: &str) -> Result<Option<String>, ClusterError> {
        use k8_client::http::StatusCode;

        let result = self
            .kube_client
            .retrieve_item::<ServiceSpec, _>(&InputObjectMeta::named("flv-sc-public", ns))
            .await;

        let svc = match result {
            Ok(svc) => svc,
            Err(k8_client::ClientError::Client(status)) if status == StatusCode::NOT_FOUND => return Ok(None),
            Err(err) => {
                return Err(ClusterError::Other(format!(
                    "unable to look up fluvio service in k8: {}",
                    err
                )))
            }
        };

        let ingress_addr = svc
            .status
            .load_balancer
            .ingress
            .iter()
            .find(|_| true)
            .and_then(|ingress| ingress.host_or_ip().to_owned());

        let address = ingress_addr.and_then(|addr| {
            svc.spec
                .ports
                .iter()
                .find(|_| true)
                .and_then(|port| port.target_port)
                .map(|target_port| format!("{}:{}", addr, target_port))
        });

        if let Some(address) = &address {
            debug!(addr = &**address, "Discovered SC address");
        }

        Ok(address)
    }

    /// Wait until the Fluvio SC public service appears in Kubernetes
    #[instrument(skip(self, ns))]
    async fn wait_for_sc_service(&self, ns: &str) -> Result<Option<String>, ClusterError> {
        use k8_client::http::StatusCode;

        let input = InputObjectMeta::named("flv-sc-public", ns);

        for i in 0..100u16 {
            match self
                .kube_client
                .retrieve_item::<ServiceSpec, _>(&input)
                .await
            {
                Ok(svc) => {
                    // check if load balancer status exists
                    if let Some(addr) = svc.status.load_balancer.find_any_ip_or_host() {
                        debug!(addr, "Found SC service load balancer");
                        return Ok(Some(format!("{}:9003", addr.to_owned())));
                    } else {
                        debug!(
                            attempt = i,
                            "SC service exists but no load balancer exist yet, continue wait"
                        );
                        sleep(Duration::from_millis(2000)).await;
                    }
                }
                Err(err) => match err {
                    K8ClientError::Client(status) if status == StatusCode::NOT_FOUND => {
                        debug!(attempt = i, "No SC service found, sleeping");
                        sleep(Duration::from_millis(2000)).await;
                    }
                    _ => panic!("error: {}", err),
                },
            };
        }

        Ok(None)
    }

    /// Wait until all SPUs are ready and have ingress
    #[instrument(skip(self, ns))]
    async fn wait_for_spu(&self, ns: &str) -> Result<bool, ClusterError> {
        // Try waiting for SPUs for 100 cycles
        for i in 0..100u16 {
            let items = self.kube_client.retrieve_items::<SpuSpec, _>(ns).await?;
            let spu_count = items.items.len();

            // Check that all items have ingress
            let ready_spu = items
                .items
                .iter()
                .filter(|spu_obj| {
                    !spu_obj.spec.public_endpoint.ingress.is_empty() && spu_obj.status.is_online()
                })
                .count();

            if spu_count == ready_spu {
                info!(spu_count, "All SPUs are ready");
                return Ok(true);
            } else {
                debug!(
                    total_expected_spu = spu_count,
                    ready_spu,
                    attempt = i,
                    "Not all SPUs are ready. Waiting",
                );
                sleep(Duration::from_millis(2000)).await;
            }
        }

        Ok(false)
    }

    /// Install server-side TLS by uploading secrets to kubernetes
    #[instrument(skip(self, paths))]
    fn upload_tls_secrets_from_files(&self, paths: &TlsPaths) -> Result<(), IoError> {
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
            .inherit();

        Command::new("kubectl")
            .args(&["delete", "secret", "fluvio-tls", "--ignore-not-found=true"])
            .args(&["--namespace", &self.config.namespace])
            .inherit();

        Command::new("kubectl")
            .args(&["create", "secret", "generic", "fluvio-ca"])
            .args(&["--from-file", ca_cert])
            .args(&["--namespace", &self.config.namespace])
            .inherit();

        Command::new("kubectl")
            .args(&["create", "secret", "tls", "fluvio-tls"])
            .args(&["--cert", server_cert])
            .args(&["--key", server_key])
            .args(&["--namespace", &self.config.namespace])
            .inherit();

        Ok(())
    }

    /// Updates the Fluvio configuration with the newly installed cluster info.
    fn update_profile(&self, external_addr: String) -> Result<(), ClusterError> {
        let mut config_file = ConfigFile::load_default_or_new()?;
        let config = config_file.mut_config();

        let profile_name = self.compute_profile_name()?;

        match config.cluster_mut(&profile_name) {
            Some(cluster) => {
                cluster.addr = external_addr;
                cluster.tls = self.config.server_tls_policy.clone();
            }
            None => {
                let mut local_cluster = FluvioConfig::new(external_addr);
                local_cluster.tls = self.config.server_tls_policy.clone();
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
    fn compute_profile_name(&self) -> Result<String, ClusterError> {
        let k8_config = K8Config::load()?;

        let kc_config = match k8_config {
            K8Config::Pod(_) => {
                return Err(ClusterError::Other(
                    "Pod config is not valid here".to_owned(),
                ))
            }
            K8Config::KubeConfig(config) => config,
        };

        kc_config
            .config
            .current_context()
            .ok_or_else(|| ClusterError::Other("No context fount".to_owned()))
            .map(|ctx| ctx.name.to_owned())
    }

    /// Provisions a SPU group for the given cluster according to internal config
    #[instrument(
        skip(self, cluster),
        fields(cluster_addr = &*cluster.addr)
    )]
    async fn create_managed_spu_group(&self, cluster: &FluvioConfig) -> Result<(), ClusterError> {
        let name = self.config.group_name.clone();
        let mut fluvio = Fluvio::connect_with_config(cluster).await?;
        let mut admin = fluvio.admin().await;
        admin
            .create(name, false, self.config.spu_spec.clone())
            .await?;

        Ok(())
    }
}
