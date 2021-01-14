use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Display;
use std::path::PathBuf;
use std::borrow::Cow;
use std::process::Command;
use std::time::Duration;
use std::net::SocketAddr;
use std::process::Stdio;
use std::fs::File;
use std::env;

use tracing::{info, warn, debug, error, instrument};
use once_cell::sync::Lazy;

use fluvio::{Fluvio, FluvioConfig};
use fluvio::metadata::spg::SpuGroupSpec;
use fluvio::metadata::spu::SpuSpec;
use fluvio::config::{TlsPolicy, TlsConfig, TlsPaths, ConfigFile, Profile};
use flv_util::cmd::CommandExt;
use fluvio_future::timer::sleep;
use fluvio_future::net::{TcpStream, resolve};
use k8_client::K8Client;
use k8_config::K8Config;
use k8_client::metadata::MetadataClient;
use k8_client::core::service::ServiceSpec;

use crate::helm::{HelmClient, Chart, InstalledChart};
use crate::check::{UnrecoverableCheck, CheckFailed, RecoverableCheck, CheckResults};
use crate::error::K8InstallError;
use crate::{
    ClusterError, StartStatus, DEFAULT_NAMESPACE, DEFAULT_CHART_SYS_REPO, DEFAULT_CHART_APP_REPO,
    CheckStatus, ClusterChecker, CheckStatuses,
};
use crate::start::{ChartLocation, DEFAULT_CHART_REMOTE};

const DEFAULT_REGISTRY: &str = "infinyon";
const DEFAULT_APP_NAME: &str = "fluvio-app";
const DEFAULT_SYS_NAME: &str = "fluvio-sys";
const DEFAULT_CHART_SYS_NAME: &str = "fluvio/fluvio-sys";
const DEFAULT_CHART_APP_NAME: &str = "fluvio/fluvio-app";
const DEFAULT_GROUP_NAME: &str = "main";
const DEFAULT_CLOUD_NAME: &str = "minikube";
const FLUVIO_SC_SERVICE: &str = "fluvio-sc-public";
/// maximum time waiting for sc service to come up
static MAX_SC_SERVICE_WAIT: Lazy<u64> = Lazy::new(|| {
    let var_value = env::var("FLV_CLUSTER_MAX_SC_SERVICE_WAIT").unwrap_or_default();
    var_value.parse().unwrap_or(30)
});
/// maximum time waiting for network check, DNS or network
static MAX_SC_NETWORK_LOOP: Lazy<u16> = Lazy::new(|| {
    let var_value = env::var("FLV_CLUSTER_MAX_SC_NETWORK_LOOP").unwrap_or_default();
    var_value.parse().unwrap_or(30)
});
const NETWORK_SLEEP_MS: u64 = 1000;
/// time betwen network check
const DELAY: u64 = 3000;

/// A builder for cluster startup options
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
    /// The authorization ConfigMap name
    authorization_config_map: Option<String>,
    /// K8 resource requiremets
    resource_requirments: Option<ResourceRequirments>,
    /// Should the pre install checks be skipped
    skip_checks: bool,
    /// if set use cluster ip instead of egress
    use_cluster_ip: bool,
    /// if set, disable spu liveness checking
    skip_spu_liveness_check: bool,
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
            kube_client: K8Client::default().map_err(K8InstallError::K8ClientError)?,
            helm_client: HelmClient::new().map_err(K8InstallError::HelmError)?,
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

    /// Whether to skip pre-install checks before installation. Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_skip_checks(true)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_skip_checks(mut self, skip_checks: bool) -> Self {
        self.skip_checks = skip_checks;
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
    pub fn with_cloud<S: Into<String>>(mut self, cloud: S) -> Self {
        self.cloud = cloud.into();
        self
    }

    /// Sets the ConfigMap for authorization.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// let installer = ClusterInstaller::new()
    ///     .with_authorization_config_map("authorization")
    ///     .build()
    ///     .unwrap();
    /// ```
    ///
    pub fn with_authorization_config_map<S: Into<String>>(
        mut self,
        authorization_config_map: S,
    ) -> Self {
        self.authorization_config_map = Some(authorization_config_map.into());
        self
    }

    /// Sets the resource requirments
    ///
    /// # Example
    ///
    /// ```no_run
    /// use fluvio_cluster::ClusterInstaller;
    /// use fluvio_cluster::{ResourceRequirments, ContainerResourceRequirments};
    /// use fluvio_cluster::{ContainerResourceRequirmentValues, MilliCpu, Memory};
    ///
    /// let resource_requirements = ResourceRequirments {
    ///     sc: Some(ContainerResourceRequirments {
    ///         limits: Some(ContainerResourceRequirmentValues {
    ///             cpu: Some(MilliCpu(1000)),
    ///             memory: Some(Memory::Megabytes(512))
    ///         }),
    ///         requests: Some(ContainerResourceRequirmentValues {
    ///             cpu: Some(MilliCpu(20)),
    ///             memory: Some(Memory::Megabytes(64))
    ///         })
    ///     }),
    ///     spu: Some(ContainerResourceRequirments {
    ///         limits: Some(ContainerResourceRequirmentValues {
    ///             cpu: Some(MilliCpu(1000)),
    ///             memory: Some(Memory::Gigabytes(1))
    ///         }),
    ///         requests: Some(ContainerResourceRequirmentValues {
    ///             cpu: Some(MilliCpu(20)),
    ///             memory: Some(Memory::Megabytes(512))
    ///         })
    ///     })
    /// };
    ///
    /// let installer = ClusterInstaller::new()
    ///     .with_resource_requirements(resource_requirements)
    ///     .build()
    ///     .unwrap();
    /// ```
    ///
    pub fn with_resource_requirements(mut self, resource_requirments: ResourceRequirments) -> Self {
        self.resource_requirments = Some(resource_requirments);
        self
    }

    /// Use cluster ip instead of load balancer for communication to SC
    /// This is is useful inside k8 cluster
    pub fn with_cluster_ip(mut self, use_cluster_ip: bool) -> Self {
        self.use_cluster_ip = use_cluster_ip;
        self
    }

    /// If set, skip spu liveness check
    pub fn with_skip_spu_livness_check(mut self, skip_checks: bool) -> Self {
        self.skip_spu_liveness_check = skip_checks;
        self
    }
}

/// Compute resource requirements for fluvio server components
#[derive(Debug)]
pub struct ResourceRequirments {
    /// Resource requirements for SC container
    pub sc: Option<ContainerResourceRequirments>,
    /// Resource requirements for SPU container
    pub spu: Option<ContainerResourceRequirments>,
}

/// Container compute resource requirements
#[derive(Debug)]
pub struct ContainerResourceRequirments {
    /// Maximum amount of compute resources allowed
    pub limits: Option<ContainerResourceRequirmentValues>,
    /// Minimum amount of compute resources required
    pub requests: Option<ContainerResourceRequirmentValues>,
}

/// K8 container resource requirement values
#[derive(Debug)]
pub struct ContainerResourceRequirmentValues {
    /// CPU
    pub cpu: Option<MilliCpu>,
    /// Memory
    pub memory: Option<Memory>,
}

/// One thousandth of a cpu
#[derive(Debug)]
pub struct MilliCpu(pub u16);

impl Display for MilliCpu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}m", self.0)
    }
}

/// Units for representing memory
#[derive(Debug)]
pub enum Memory {
    /// Memory in Kilobytes
    Kilobytes(u64),
    /// Memory in Megabytes
    Megabytes(u64),
    /// Memory in Gigabytes
    Gigabytes(u64),
    /// Memory in Terabytes
    Terabytes(u64),
    /// Memory in Petabytes
    Petabytes(u64),
    /// Memory in Exabytes
    Exabytes(u64),
}

impl Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Memory::Kilobytes(value) => write!(f, "{}Ki", value),
            Memory::Megabytes(value) => write!(f, "{}Mi", value),
            Memory::Gigabytes(value) => write!(f, "{}Gi", value),
            Memory::Terabytes(value) => write!(f, "{}Ti", value),
            Memory::Petabytes(value) => write!(f, "{}Pi", value),
            Memory::Exabytes(value) => write!(f, "{}Ei", value),
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
            chart_version: crate::VERSION.trim().to_string(),
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
            authorization_config_map: None,
            resource_requirments: None,
            skip_checks: false,
            use_cluster_ip: false,
            skip_spu_liveness_check: false,
        }
    }

    /// Get all the available versions of fluvio chart
    pub fn versions() -> Result<Vec<Chart>, ClusterError> {
        let helm_client = HelmClient::new().map_err(K8InstallError::HelmError)?;
        let versions = helm_client
            .versions(DEFAULT_CHART_APP_NAME)
            .map_err(K8InstallError::HelmError)?;
        Ok(versions)
    }

    /// Get installed system chart
    pub fn sys_charts() -> Result<Vec<InstalledChart>, K8InstallError> {
        let helm_client = HelmClient::new()?;
        let sys_charts = helm_client.get_installed_chart_by_name(DEFAULT_CHART_SYS_REPO, None)?;
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
    #[instrument(skip(self))]
    pub async fn setup(&self) -> CheckResults {
        let fix = |err| self.pre_install_fix(err);
        ClusterChecker::empty()
            .with_k8_checks()
            .run_wait_and_fix(fix)
            .await
    }

    async fn _try_minikube_tunnel(&self) -> Result<(), K8InstallError> {
        let log_file = File::create("/tmp/tunnel.out")?;
        let error_file = log_file.try_clone()?;

        // run minikube tunnel  > /tmp/tunnel.out 2> /tmp/tunnel.out
        Command::new("minikube")
            .arg("tunnel")
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(error_file))
            .spawn()?;
        sleep(Duration::from_millis(DELAY)).await;
        Ok(())
    }

    /// Given a pre-check error, attempt to automatically correct it
    #[instrument(skip(self, error))]
    pub(crate) async fn pre_install_fix(
        &self,
        error: RecoverableCheck,
    ) -> Result<(), UnrecoverableCheck> {
        // Depending on what error occurred, try to fix the error.
        // If we handle the error successfully, return Ok(()) to indicate success
        // If we cannot handle this error, wrap it in UnrecoverableCheck::FailedRecovery
        match error {
            RecoverableCheck::MissingSystemChart if self.config.install_sys => {
                println!("Fluvio system chart not installed. Attempting to install");
                self._install_sys()
                    .map_err(|_| UnrecoverableCheck::FailedRecovery(error))?;
            }
            RecoverableCheck::MinikubeTunnelNotFoundRetry => {
                println!(
                    "Load balancer service is not available, trying to bring up minikube tunnel"
                );
                self._try_minikube_tunnel()
                    .await
                    .map_err(|_| UnrecoverableCheck::FailedRecovery(error))?;
            }
            unhandled => {
                warn!("Pre-install was unable to autofix an error");
                return Err(UnrecoverableCheck::FailedRecovery(unhandled));
            }
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

        self.install_app()?;
        let namespace = &self.config.namespace;
        let (address, port) = self
            .wait_for_sc_service(namespace)
            .await
            .map_err(|_| K8InstallError::UnableToDetectService)?;
        info!(addr = %address, "Fluvio SC is up:");

        if self.config.save_profile {
            self.update_profile(address.clone())?;
        }

        if self.config.spu_spec.replicas > 0 {
            debug!("waiting for SC to spin up");
            // Wait a little bit for the SC to spin up
            sleep(Duration::from_millis(2000)).await;

            // Create a managed SPU cluster
            let cluster =
                FluvioConfig::new(address.clone()).with_tls(self.config.client_tls_policy.clone());
            self.create_managed_spu_group(&cluster).await?;

            // Wait for the SPU cluster to spin up
            if !self.config.skip_spu_liveness_check {
                self.wait_for_spu(namespace, self.config.spu_spec.replicas)
                    .await?;
            }
        }

        Ok(StartStatus {
            address,
            port,
            checks,
        })
    }

    /// Install the Fluvio System chart on the configured cluster
    #[doc(hidden)]
    #[instrument(skip(self))]
    pub fn _install_sys(&self) -> Result<(), K8InstallError> {
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
                let chart_location = chart_home.join(DEFAULT_SYS_NAME);
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
    fn install_app(&self) -> Result<(), K8InstallError> {
        debug!(
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
        let mut install_settings: Vec<(_, Cow<str>)> = vec![
            ("image.registry", Cow::Borrowed(&self.config.image_registry)),
            ("image.tag", Cow::Borrowed(&fluvio_tag)),
            ("cloud", Cow::Borrowed(&self.config.cloud)),
        ];

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

        const SC_CPU_REQUEST: &str = "scResources.requests.cpu";
        const SC_MEM_REQUEST: &str = "scResources.requests.memory";
        const SC_CPU_LIMIT: &str = "scResources.limit.cpu";
        const SC_MEM_LIMIT: &str = "scResources.limit.memory";
        const SPU_CPU_REQUEST: &str = "spuResources.requests.cpu";
        const SPU_MEM_REQUEST: &str = "spuResources.requests.memory";
        const SPU_CPU_LIMIT: &str = "spuResources.limit.cpu";
        const SPU_MEM_LIMIT: &str = "spuResources.limit.memory";

        if let Some(ref resource_requirments) = &self.config.resource_requirments {
            if let Some(ref sc_resource_requirments) = resource_requirments.sc {
                if let Some(ref requests) = sc_resource_requirments.requests {
                    if let Some(ref cpu) = requests.cpu {
                        install_settings.push((SC_CPU_REQUEST, Cow::Owned(cpu.to_string())));
                    }
                    if let Some(ref memory) = requests.memory {
                        install_settings.push((SC_MEM_REQUEST, Cow::Owned(memory.to_string())));
                    }
                }
                if let Some(ref limits) = sc_resource_requirments.limits {
                    if let Some(ref cpu) = limits.cpu {
                        install_settings.push((SC_CPU_LIMIT, Cow::Owned(cpu.to_string())));
                    }
                    if let Some(ref memory) = limits.memory {
                        install_settings.push((SC_MEM_LIMIT, Cow::Owned(memory.to_string())));
                    }
                }
            }
            if let Some(ref spu_resource_requirments) = resource_requirments.spu {
                if let Some(ref requests) = spu_resource_requirments.requests {
                    if let Some(ref cpu) = requests.cpu {
                        install_settings.push((SPU_CPU_REQUEST, Cow::Owned(cpu.to_string())));
                    }
                    if let Some(ref memory) = requests.memory {
                        install_settings.push((SPU_MEM_REQUEST, Cow::Owned(memory.to_string())));
                    }
                }
                if let Some(ref limits) = spu_resource_requirments.limits {
                    if let Some(ref cpu) = limits.cpu {
                        install_settings.push((SPU_CPU_LIMIT, Cow::Owned(cpu.to_string())));
                    }
                    if let Some(ref memory) = limits.memory {
                        install_settings.push((SPU_MEM_LIMIT, Cow::Owned(memory.to_string())));
                    }
                }
            }
        }

        use std::borrow::Borrow;
        let install_settings = install_settings
            .iter()
            .map(|(k, v)| (*k, v.borrow()))
            .collect::<Vec<(&str, &str)>>();

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
                    return Err(K8InstallError::HelmChartNotFound(format!(
                        "{}:{}",
                        &self.config.chart_name, &self.config.chart_version
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
                let chart_location = chart_home.join(DEFAULT_APP_NAME);
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
    async fn discover_sc_address(&self, ns: &str) -> Result<Option<(String, u16)>, K8InstallError> {
        use tokio::select;
        use futures_lite::stream::StreamExt;

        use fluvio_future::timer::sleep;
        use k8_client::core::metadata::K8Watch;

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
                                        .find(|_| true)
                                        .and_then(|port| port.target_port).expect("target port should be there");

                                    if self.config.use_cluster_ip  {
                                        return Ok(Some((format!("{}:{}",service.spec.cluster_ip,target_port),target_port)))
                                    };

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
        info!(sock_addr = %sock_addr_str, "waiting for SC port check");
        for i in 0..*MAX_SC_NETWORK_LOOP {
            let sock_addr = self.wait_for_sc_dns(&sock_addr_str).await?;
            if TcpStream::connect(&*sock_addr).await.is_ok() {
                info!(sock_addr = %sock_addr_str, "finished SC port check");
                return Ok(());
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
    async fn wait_for_spu(&self, ns: &str, spu: u16) -> Result<bool, K8InstallError> {
        info!("waiting for SPU");
        for i in 0..*MAX_SC_NETWORK_LOOP {
            debug!("retrieving spu specs");
            let items = self.kube_client.retrieve_items::<SpuSpec, _>(ns).await?;
            let spu_count = items.items.len();

            // Check that all items have ingress
            let ready_spu = items
                .items
                .iter()
                .filter(|spu_obj| {
                    // if cluster ip is used then we skip checkking ingress
                    (self.config.use_cluster_ip || !spu_obj.spec.public_endpoint.ingress.is_empty())
                        && spu_obj.status.is_online()
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
                info!(
                    attempt = i,
                    "{} of {} spu ready, sleeping for {} ms", ready_spu, spu, NETWORK_SLEEP_MS
                );
                sleep(Duration::from_millis(NETWORK_SLEEP_MS)).await;
            }
        }

        Err(K8InstallError::SPUTimeout)
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
    fn update_profile(&self, external_addr: String) -> Result<(), K8InstallError> {
        debug!("updating profile for: {}", external_addr);
        let mut config_file = ConfigFile::load_default_or_new()?;
        let config = config_file.mut_config();

        let profile_name = self.compute_profile_name()?;

        match config.cluster_mut(&profile_name) {
            Some(cluster) => {
                cluster.addr = external_addr;
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
    fields(cluster_addr = & * cluster.addr)
    )]
    async fn create_managed_spu_group(&self, cluster: &FluvioConfig) -> Result<(), K8InstallError> {
        debug!("trying to create managed spu: {:#?}", cluster);
        let name = self.config.group_name.clone();
        let fluvio = Fluvio::connect_with_config(cluster).await?;
        let mut admin = fluvio.admin().await;
        admin
            .create(name, false, self.config.spu_spec.clone())
            .await?;

        Ok(())
    }
}
