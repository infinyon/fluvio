use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::borrow::Cow;
use std::process::Command;
use std::time::Duration;
use std::net::SocketAddr;

use tracing::{info, warn, debug, trace, instrument};
use fluvio::{Fluvio, FluvioConfig};
use fluvio::metadata::spg::SpuGroupSpec;
use fluvio::metadata::spu::SpuSpec;
use fluvio::config::{TlsPolicy, TlsConfig, TlsPaths, ConfigFile, Profile};
use flv_util::cmd::CommandExt;
use fluvio_future::timer::sleep;
use fluvio_future::net::{TcpStream, resolve};
use k8_client::K8Client;
use k8_config::K8Config;
use k8_config::context::MinikubeContext;
use k8_client::metadata::MetadataClient;
use k8_obj_core::service::ServiceSpec;
use k8_obj_metadata::InputObjectMeta;

use crate::ClusterError;
use crate::helm::{HelmClient, Chart, InstalledChart};
use crate::check::{check_cluster_server_host, CheckError, check_helm_version, check_system_chart, check_already_installed};

const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_REGISTRY: &str = "infinyon";
const DEFAULT_CHART_SYS_REPO: &str = "fluvio-sys";
const DEFAULT_CHART_SYS_NAME: &str = "fluvio/fluvio-sys";
const DEFAULT_CHART_APP_REPO: &str = "fluvio";
const DEFAULT_CHART_APP_NAME: &str = "fluvio/fluvio-app";
const DEFAULT_CHART_REMOTE: &str = "https://charts.fluvio.io";
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
    /// The location to find the fluvio charts
    chart_location: ChartLocation,
    /// The name of the SPU group to create
    group_name: String,
    /// The name of the Fluvio cloud
    cloud: String,
    /// Whether to save an update to the Fluvio profile
    save_profile: bool,
    /// Whether to install fluvio-sys with fluvio-app
    install_sys: bool,
    /// Whether to update the `kubectl` context
    update_context: bool,
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
    /// # use fluvio_cluster::ClusterInstaller;
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_namespace("my-namespace")
    ///     .build()
    ///     .expect("should build installer");
    /// ```
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
    /// # use fluvio_cluster::ClusterInstaller;
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
    /// # use fluvio_cluster::ClusterInstaller;
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
    /// # use fluvio_cluster::ClusterInstaller;
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
    /// a local filesystem path. The path given is expected to be the parent directory
    /// of both the `fluvio-app` and `fluvio-sys` charts.
    ///
    /// This option is mutually exclusive from [`with_remote_chart`]; if both are used,
    /// the latest one defined is the one that's used.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_local_chart("./k8-util/helm")
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
    /// This is the default case, with the default location being `https://charts.fluvio.io`,
    /// where official Fluvio helm charts are located. Remote helm charts are expected
    /// to be a valid URL.
    ///
    /// This option is mutually exclusive from [`with_local_chart`]; if both are used,
    /// the latest one defined is the one that's used.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_remote_chart("https://charts.fluvio.io")
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
    /// # use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_save_profile(true)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_save_profile(mut self, save_profile: bool) -> Self {
        self.save_profile = save_profile;
        self
    }

    /// Whether to install the `fluvio-sys` chart in the full installation. Defaults to `true`.
    ///
    /// # Example
    ///
    /// If you want to disable installing the system chart, you can do this
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_system_chart(false)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_system_chart(mut self, install_sys: bool) -> Self {
        self.install_sys = install_sys;
        self
    }

    /// Whether to update the `kubectl` context to match the Fluvio installation. Defaults to `true`.
    ///
    /// # Example
    ///
    /// If you do not want your Kubernetes contexts to be updated, you can do this
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_update_context(false)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_update_context(mut self, update_context: bool) -> Self {
        self.update_context = update_context;
        self
    }

    /// Sets the number of SPU replicas that should be provisioned. Defaults to 1.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterInstaller;
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
    /// # use fluvio_cluster::ClusterInstaller;
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

    /// Sets the K8 cluster cloud environment.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_cloud("minikube")
    ///     .build()
    ///     .unwrap();
    /// ```
    ///
    /// [`RUST_LOG`]: https://docs.rs/tracing-subscriber/0.2.11/tracing_subscriber/filter/struct.EnvFilter.html
    pub fn with_cloud<S: Into<String>>(mut self, cloud: S) -> Self {
        self.cloud = cloud.into();
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
/// If you want to try out Fluvio on Kubernetes, you can use [Minikube]
/// as an installation target. This is the default target that the
/// `ClusterInstaller` uses, so it doesn't require any complex setup.
///
/// # Example
///
/// To install Fluvio using all the default settings, use
/// `ClusterInstaller::new()`
///
/// ```no_run
/// # use fluvio_cluster::ClusterInstaller;
/// let installer = ClusterInstaller::new()
///     .build()
///     .expect("should initialize installer");
///
/// // Installing Fluvio is asynchronous, so you'll need an async runtime
/// let result = fluvio_future::task::run_block_on(async {
///     installer.install_fluvio().await
/// });
/// ```
///
/// [Helm Charts]: https://helm.sh/
/// [Minikube]: https://kubernetes.io/docs/tasks/tools/install-minikube/
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
            install_sys: true,
            update_context: false,
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

    /// Checks if all of the prerequisites for installing Fluvio are met
    ///
    /// This will attempt to automatically fix any missing prerequisites,
    /// depending on the installer configuration. See the following options
    /// for more details:
    ///
    /// - [`with_system_chart`]
    /// - [`with_update_context`]
    ///
    /// [`with_system_chart`]: ./struct.ClusterInstaller.html#method.with_system_chart
    /// [`with_update_context`]: ./struct.ClusterInstaller.html#method.with_update_context
    fn pre_install(&self) -> Result<(), ClusterError> {

        // Continue fixing pre-check errors until we resolve all problems
        // or there is an error that we cannot fix
        loop {
            let check_error = match self.pre_install_check() {
                // This is the successful exit case. If all pre-checks succeed,
                // we can continue the installation process normally
                Ok(_) => return Ok(()),
                Err(err) => err,
            };

            // If this returns an error, we require user intervention
            self.pre_install_fix(check_error)?;

            // If we reach this point, the fix succeeded.
            // In this case, continue the loop to check for more errors
        }
    }

    /// Runs pre install checks
    ///  1. Check if compatible helm version is installed
    ///  2. Check if compatible sys charts are installed
    ///  3. Check if the K8 config hostname is not an IP address
    fn pre_install_check(&self) -> Result<(), CheckError> {
        check_helm_version(&self.helm_client, DEFAULT_HELM_VERSION)?;
        check_system_chart(&self.helm_client, DEFAULT_CHART_SYS_REPO)?;
        check_already_installed(&self.helm_client, DEFAULT_CHART_APP_REPO)?;
        check_cluster_server_host()?;
        Ok(())
    }

    /// Given a pre-check error, attempt to automatically correct it
    #[instrument(skip(self, error))]
    fn pre_install_fix(&self, error: CheckError) -> Result<(), ClusterError> {

        // Depending on what error occurred, try to fix the error.
        // If we handle the error successfully, return Ok(()) to indicate success
        // If we cannot handle this error, return it to bubble up
        match error {
            CheckError::MissingSystemChart if self.config.install_sys => {
                debug!("Fluvio system chart not installed. Attempting to install");
                self._install_sys()?;
            }
            CheckError::InvalidMinikubeContext if self.config.update_context => {
                debug!("Updating to minikube context");
                let context = MinikubeContext::try_from_system()?;
                context.save()?;
            }
            unhandled => {
                warn!("Pre-install was unable to autofix an error");
                return Err(unhandled.into());
            },
        }

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
        // Checks if env is ready for install and tries to fix anything it can
        match self.pre_install() {
            // If all checks pass, perform the main installation
            Ok(()) => self.install_app()?,
            // If Fluvio is already installed, skip install step
            Err(ClusterError::PreCheckError { source: CheckError::AlreadyInstalled }) => (),
            // If there were other unhandled errors, return them
            Err(unhandled) => return Err(unhandled),
        }

        let namespace = &self.config.namespace;
        let sc_address = match self.wait_for_sc_service(namespace).await {
            Ok(addr) => {
                info!(addr = &*addr, "Fluvio SC is up");
                addr
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
            self.wait_for_spu(namespace, self.config.spu_spec.replicas)
                .await?;
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
            ChartLocation::Local(chart_home) => {
                let chart_location = chart_home.join("fluvio-sys");
                let chart_string = chart_location.to_string_lossy();
                debug!(
                    chart_location = chart_string.as_ref(),
                    "Using local helm chart:"
                );
                self.helm_client.install(
                    &self.config.namespace,
                    DEFAULT_CHART_SYS_REPO,
                    chart_string.as_ref(),
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
            ChartLocation::Local(chart_home) => {
                let chart_location = chart_home.join("fluvio-app");
                let chart_string = chart_location.to_string_lossy();
                debug!(
                    chart_location = chart_string.as_ref(),
                    "Using local helm chart:"
                );
                self.helm_client.install(
                    &self.config.namespace,
                    DEFAULT_CHART_APP_REPO,
                    chart_string.as_ref(),
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
            Err(k8_client::ClientError::Client(status)) if status == StatusCode::NOT_FOUND => {
                info!("no SC service found");
                return Ok(None);
            }
            Err(err) => return Err(ClusterError::from(err)),
        };

        let ingress_addr = svc
            .status
            .load_balancer
            .ingress
            .iter()
            .find(|_| true)
            .and_then(|ingress| ingress.host_or_ip().to_owned());

        let sock_addr = ingress_addr.and_then(|addr| {
            svc.spec
                .ports
                .iter()
                .find(|_| true)
                .and_then(|port| port.target_port)
                .map(|target_port| format!("{}:{}", addr, target_port))
        });

        Ok(sock_addr)
    }

    /// Wait until the Fluvio SC public service appears in Kubernetes
    #[instrument(skip(self, ns))]
    async fn wait_for_sc_service(&self, ns: &str) -> Result<String, ClusterError> {
        info!("waiting for SC service");
        for i in 0..12 {
            if let Some(sock_addr) = self.discover_sc_address(ns).await? {
                info!(%sock_addr, "found SC service load balancer, discovered SC address");
                self.wait_for_sc_port_check(&sock_addr).await?;
                return Ok(sock_addr);
            }

            let sleep_ms = 1000 * 2u64.pow(i as u32);
            info!(
                attempt = i,
                "no SC service found, sleeping for {} ms", sleep_ms
            );
            sleep(Duration::from_millis(sleep_ms)).await
        }

        Err(ClusterError::SCServiceTimeout)
    }

    /// Wait until the Fluvio SC public service appears in Kubernetes
    async fn wait_for_sc_port_check(&self, sock_addr_str: &str) -> Result<(), ClusterError> {
        info!(sock_addr = %sock_addr_str, "waiting for SC port check");
        for i in 0..12 {
            let sock_addr = self.wait_for_sc_dns(&sock_addr_str).await?;
            if TcpStream::connect(&*sock_addr).await.is_ok() {
                return Ok(());
            }
            let sleep_ms = 1000 * 2u64.pow(i as u32);
            info!(attempt = i, "sc port closed, sleeping for {} ms", sleep_ms);
            sleep(Duration::from_millis(sleep_ms)).await
        }

        Err(ClusterError::SCPortCheckTimeout)
    }

    /// Wait until the Fluvio SC public service appears in Kubernetes
    async fn wait_for_sc_dns(
        &self,
        sock_addr_string: &str,
    ) -> Result<Vec<SocketAddr>, ClusterError> {
        info!("waiting for SC dns resolution");
        for i in 0..12 {
            match resolve(sock_addr_string).await {
                Ok(sock_addr) => return Ok(sock_addr),
                Err(err) => {
                    let sleep_ms = 1000 * 2u64.pow(i as u32);
                    info!(
                        attempt = i,
                        "SC dns resoultion failed {}, sleeping for {} ms", err, sleep_ms
                    );
                    sleep(Duration::from_millis(sleep_ms)).await
                }
            }
        }

        Err(ClusterError::SCDNSTimeout)
    }

    /// Wait until all SPUs are ready and have ingress
    #[instrument(skip(self, ns))]
    async fn wait_for_spu(&self, ns: &str, spu: u16) -> Result<bool, ClusterError> {
        info!("waiting for SPU");
        for i in 0..12 {
            debug!("retrieving spu specs");
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

            if spu as usize == ready_spu {
                info!(spu_count, "All SPUs are ready");
                return Ok(true);
            } else {
                debug!(
                    total_expected_spu = spu_count,
                    ready_spu,
                    attempt = i,
                    "Not all SPUs are ready. Waiting",
                );
                let sleep_ms = 1000 * 2u64.pow(i as u32);
                info!(
                    attempt = i,
                    "{} of {} spu ready, sleeping for {} ms", ready_spu, spu, sleep_ms
                );
                sleep(Duration::from_millis(sleep_ms)).await;
            }
        }

        Err(ClusterError::SPUTimeout)
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
