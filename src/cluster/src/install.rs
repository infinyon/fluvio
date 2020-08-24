use std::io::Error as IoError;
use std::io::ErrorKind;
use std::borrow::Cow;
use std::process::{Command, Stdio};
use std::time::Duration;

use tracing::{info, warn, debug, instrument};
use fluvio::{ClusterConfig, ClusterClient};
use fluvio::metadata::spg::SpuGroupSpec;
use fluvio::metadata::spu::SpuSpec;
use fluvio::config::{TlsPolicy, TlsConfig, TlsPaths, ConfigFile, Profile};
use flv_util::cmd::CommandExt;
use flv_future_aio::timer::sleep;
use k8_client::{K8Client, ClientError as K8ClientError};
use k8_config::K8Config;
use k8_client::metadata::MetadataClient;
use k8_obj_core::service::ServiceSpec;
use k8_obj_metadata::InputObjectMeta;

use crate::ClusterError;
use crate::helm::HelmClient;

/// Describes the Kubernetes options of an installation
#[derive(Debug)]
pub struct KubeOptions {
    /// The namespace to install under
    pub namespace: String,
    /// The name of the SPU group to create
    pub group_name: String,
    /// The name of the helm chart to install
    pub chart_name: String,
    pub cloud: String,
    /// Use a specific version of kubernetes
    pub version: Option<String>,
    /// The location to find the helm chart
    pub chart_location: Option<String>,
}

impl Default for KubeOptions {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            group_name: "main".to_string(),
            chart_name: "fluvio".to_string(),
            cloud: "minikube".to_string(),
            version: None,
            chart_location: None,
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
///     .expect("should initialize installer");
///
/// // Installing Fluvio is asynchronous, so you'll need an async runtime
/// let result = async_std::task::block_on(async {
///     installer.install_fluvio().await
/// });
/// ```
#[derive(Debug)]
pub struct ClusterInstaller {
    /// Whether to operate in development mode
    develop: bool,
    /// Whether to skip updating the profile
    skip_profile: bool,
    /// How much storage to allocate on SPUs
    spu_spec: SpuGroupSpec,
    /// The logging settings to set in the cluster
    rust_log: Option<String>,
    /// Whether to configure install to use TLS
    tls_policy: TlsPolicy,
    /// Kubernetes options to pass to helm
    kube_options: KubeOptions,
    /// Shared Kubernetes client for install
    kube_client: K8Client,
    /// Helm client for performing installs
    helm: HelmClient,
}

impl ClusterInstaller {
    /// Create a cluster installer with default settings
    pub fn new() -> Result<Self, ClusterError> {
        let spu_spec = SpuGroupSpec {
            replicas: 1,
            min_id: 1,
            ..SpuGroupSpec::default()
        };

        Ok(Self {
            develop: false,
            skip_profile: false,
            spu_spec,
            rust_log: None,
            tls_policy: TlsPolicy::Disabled,
            kube_options: Default::default(),
            kube_client: K8Client::default()?,
            helm: HelmClient::new()?,
        })
    }

    /// Configures installer to operate in development mode
    pub fn with_develop(mut self, develop: bool) -> Self {
        self.develop = develop;
        self
    }

    /// Configures the installer to skip writing a new profile to disk
    pub fn with_skip_profile(mut self, skip_profile: bool) -> Self {
        self.skip_profile = skip_profile;
        self
    }

    /// Configures settings for the SPU group the installer will create
    pub fn with_spu_group_spec(mut self, spu_spec: SpuGroupSpec) -> Self {
        self.spu_spec = spu_spec;
        self
    }

    /// Configure installer to use the given RUST_LOG in the new SC
    pub fn with_log(mut self, log: String) -> Self {
        self.rust_log = Some(log);
        self
    }

    /// Configure installer to use the given TLS configuration
    pub fn with_tls<T: Into<TlsPolicy>>(mut self, tls: T) -> Self {
        self.tls_policy = tls.into();
        self
    }

    /// Configure installer to use the given Kubernetes options
    pub fn with_kube_options(mut self, kube_options: KubeOptions) -> Self {
        self.kube_options = kube_options;
        self
    }

    /// Installs Fluvio according to the installer's configuration
    ///
    /// Returns the external address of the new cluster's SC
    #[instrument(
        skip(self),
        fields(namespace = &*self.kube_options.namespace),
    )]
    pub async fn install_fluvio(&self) -> Result<String, ClusterError> {
        self.install_core()?;

        let namespace = &self.kube_options.namespace;
        let sc_address = match self.wait_for_sc_service(namespace).await {
            Ok(Some(addr)) => {
                info!(addr = &*addr, "Fluvio SC is up");
                addr
            }
            Ok(None) => {
                warn!("Timed out when waiting for SC service");
                return Err(ClusterError::Other("Timed out when waiting for SC service".to_string()));
            }
            Err(err) => {
                warn!("Unable to detect Fluvio service. If you're running on Minikube, make sure you have the tunnel up!");
                return Err(ClusterError::Other(format!("Unable to detect Fluvio service: {}", err)));
            }
        };

        if !self.skip_profile {
            self.update_profile(sc_address.clone())?;
        }

        if self.spu_spec.replicas > 0 {

            // Wait a little bit for the SC to spin up
            sleep(Duration::from_millis(2000)).await;

            // Create a managed SPU cluster
            let cluster = ClusterConfig::new(sc_address.clone())
                .with_tls(self.tls_policy.clone());
            self.create_managed_spu_group(cluster).await?;

            // Wait for the SPU cluster to spin up
            if !self.wait_for_spu(namespace).await? {
                warn!("SPU took too long to get ready");
                return Err(ClusterError::Other("SPU took too long to get ready".to_string()));
            }
        }

        Ok(sc_address)
    }

    /// Install the Fluvio System chart on the configured cluster
    #[instrument(skip(self))]
    pub fn install_sys(&self) -> Result<(), ClusterError> {
        let kube = &self.kube_options;
        let chart_location = kube.chart_location
            .as_deref()
            .unwrap_or("https://infinyon.github.io/charts");
        self.helm.repo_add("fluvio", chart_location);
        self.helm.repo_update();
        self.helm.install("fluvio-sys", "fluvio/fluvio-sys", &[&format!("cloud={}", kube.cloud)])?;
        info!("Fluvio sys chart has been installed");
        Ok(())
    }

    /// Install Fluvio Core chart on the configured cluster
    #[instrument(skip(self))]
    pub fn install_core(&self) -> Result<(), ClusterError> {

        // If configured with TLS, copy certs to server
        if let TlsPolicy::Verified(tls) = &self.tls_policy {
            self.upload_tls_secrets(tls)?;
        }

        let registry = if self.develop {
            "localhost:5000/infinyon"
        } else {
            "infinyon"
        };

        let kube = &self.kube_options;
        let version = self.get_install_version()?;
        let fluvio_version = format!("fluvioVersion={}", version);
        let chart_location = kube.chart_location
            .as_deref()
            .unwrap_or("./k8-util/helm/fluvio-core");

        // Command will have args appended mutably then run at end
        let mut cmd = Command::new("helm");

        if self.develop {
            cmd
                .args(&["install", &kube.chart_name, chart_location])
                .args(&["--set", &fluvio_version])
                .args(&["--set", &format!("registry={}", registry)]);
        } else {

            const CORE_CHART_NAME: &str = "fluvio/fluvio-core";
            let chart_location = kube.chart_location
                .as_deref()
                .unwrap_or("https://infinyon.github.io/charts");

            self.helm.repo_add("fluvio", chart_location);
            self.helm.repo_update();

            if !self.helm.chart_version_exists(CORE_CHART_NAME, crate::VERSION)? {
                return Err(ClusterError::Other(format!(
                    "{}:{} not found in helm repo",
                    CORE_CHART_NAME,
                    crate::VERSION
                )));
            }

            let version = kube.version
                .as_deref()
                .unwrap_or(crate::VERSION);

            cmd
                .args(&["install", &kube.chart_name, CORE_CHART_NAME])
                .args(&["--version", version]);
        }

        let ns = &kube.namespace;
        cmd.args(&["-n", ns, "--set", &format!("cloud={}", kube.cloud)]);

        // If TLS is enabled, set it as a helm variable
        if let TlsPolicy::Anonymous | TlsPolicy::Verified(_) = self.tls_policy {
            cmd.args(&["--set", "tls=true"]);
        }

        // If RUST_LOG is defined, pass it to SC
        if let Some(log) = &self.rust_log {
            cmd.args(&["--set", &format!("scLog={}", log)]);
        }

        cmd.wait();

        info!("Fluvio core chart has been installed");

        Ok(())
    }

    /// Uploads TLS secrets to Kubernetes
    pub fn upload_tls_secrets(&self, tls: &TlsConfig) -> Result<(), IoError> {
        let paths: Cow<TlsPaths> = match tls {
            TlsConfig::Files(paths) => Cow::Borrowed(paths),
            TlsConfig::Inline(certs) => Cow::Owned(certs.try_into_temp_files()?),
        };
        self.upload_tls_secrets_from_files(paths.as_ref())?;
        Ok(())
    }

    /// Looks up the external address of a Fluvio SC instance in the given namespace
    #[instrument(skip(self, ns))]
    pub async fn discover_sc_address(&self, ns: &str) -> Result<Option<String>, ClusterError> {
        let result = self.kube_client
            .retrieve_item::<ServiceSpec, _>(&InputObjectMeta::named("flv-sc-public", ns))
            .await;

        let svc = match result {
            Ok(svc) => svc,
            Err(k8_client::ClientError::NotFound) => return Ok(None),
            Err(err) => return Err(ClusterError::Other(format!(
                "unable to look up fluvio service in k8: {}", err
            ))),
        };

        let ingress_addr = svc.status.load_balancer.ingress
            .iter()
            .find(|_| true)
            .and_then(|ingress| ingress.host_or_ip().to_owned());

        let address = ingress_addr
            .and_then(|addr| {
                svc.spec.ports.iter().find(|_| true)
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
    pub async fn wait_for_sc_service(&self, ns: &str) -> Result<Option<String>, ClusterError> {
        let input = InputObjectMeta::named("flv-sc-public", ns);

        for i in 0..100u16 {
            match self.kube_client.retrieve_item::<ServiceSpec, _>(&input).await {
                Ok(svc) => {
                    // check if load balancer status exists
                    if let Some(addr) = svc.status.load_balancer.find_any_ip_or_host() {
                        debug!(addr, "Found SC service load balancer");
                        return Ok(Some(format!("{}:9003", addr.to_owned())));
                    } else {
                        debug!(attempt = i, "SC service exists but no load balancer exist yet, continue wait");
                        sleep(Duration::from_millis(2000)).await;
                    }
                }
                Err(err) => match err {
                    K8ClientError::NotFound => {
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
    #[instrument(
        skip(self, ns),
        fields(namespace = ns),
    )]
    pub async fn wait_for_spu(&self, ns: &str) -> Result<bool, ClusterError> {

        // Try waiting for SPUs for 100 cycles
        for i in 0..100u16 {
            let items = self.kube_client.retrieve_items::<SpuSpec, _>(ns).await?;
            let spu_count = items.items.len();

            // Check that all items have ingress
            let ready_spu = items.items.iter()
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

    /// Get the version name to label this installation with
    fn get_install_version(&self) -> Result<String, IoError> {
        if self.develop {
            let output = Command::new("git")
                .args(&["rev-parse", "HEAD"])
                .output()?;
            let hash = String::from_utf8(output.stdout)
                .map_err(|e| IoError::new(ErrorKind::InvalidData, "failed to read git hash"))?;
            Ok(hash)
        } else {
            let chart_location = self.kube_options.chart_location
                .as_deref()
                .unwrap_or("https://infinyon.github.io/charts");
            self.helm.repo_add("fluvio", chart_location);
            self.helm.repo_update();
            Ok(crate::VERSION.to_owned())
        }
    }

    /// Install server-side TLS by uploading secrets to kubernetes
    #[instrument(skip(self, paths))]
    fn upload_tls_secrets_from_files(&self, paths: &TlsPaths) -> Result<(), IoError> {
        let ca_cert = paths.ca_cert.to_str()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidInput, "ca_cert must be a valid path"))?;
        let server_cert = paths.cert.to_str()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidInput, "server_cert must be a valid path"))?;
        let server_key = paths.key.to_str()
            .ok_or_else(|| IoError::new(ErrorKind::InvalidInput, "server_key must be a valid path"))?;
        debug!("Using TLS from paths: {:?}", paths);

        let ca_status = Command::new("kubectl")
            .args(&["create", "secret", "generic", "fluvio-ca"])
            .args(&["--from-file", ca_cert])
            .args(&["--namespace", &self.kube_options.namespace])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .print()
            .status()?;
        if !ca_status.success() {
            return Err(IoError::new(ErrorKind::Other, "failed to install fluvio-ca"));
        }

        let cert_status = Command::new("kubectl")
            .args(&["create", "secret", "tls", "fluvio-tls"])
            .args(&["--cert", server_cert])
            .args(&["--key", server_key])
            .args(&["--namespace", &self.kube_options.namespace])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()?;
        if !cert_status.success() {
            return Err(IoError::new(ErrorKind::Other, "failed to install fluvio-tls"));
        }

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
                cluster.tls = self.tls_policy.clone();
            }
            None => {
                let mut local_cluster = ClusterConfig::new(external_addr);
                local_cluster.tls = self.tls_policy.clone();
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

        Ok(())
    }

    /// Determines a profile name from the name of the active Kubernetes context
    fn compute_profile_name(&self) -> Result<String, ClusterError> {
        let k8_config = K8Config::load()?;

        let kc_config = match k8_config {
            K8Config::Pod(_) => return Err(ClusterError::Other("Pod config is not valid here".to_owned())),
            K8Config::KubeConfig(config) => config,
        };

        kc_config.config.current_context()
            .ok_or_else(|| ClusterError::Other("No context fount".to_owned()))
            .map(|ctx| ctx.name.to_owned())
    }

    /// Provisions a SPU group for the given cluster according to internal config
    #[instrument(
        skip(self, cluster),
        fields(cluster_addr = &*cluster.addr)
    )]
    async fn create_managed_spu_group(&self, cluster: ClusterConfig) -> Result<(), ClusterError> {
        let name = self.kube_options.group_name.clone();
        let mut target = ClusterClient::connect(cluster).await?;
        let mut admin = target.admin().await;
        admin.create(name, false, self.spu_spec.clone()).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // #[test]
    fn test_install_cluster() {
        flv_util::init_tracer(None);

        let client_dir: PathBuf = env!("CARGO_MANIFEST_DIR").into();
        let chart_dir = client_dir.parent()
            .and_then(|dir| dir.parent())
            .map(|dir| dir.to_owned().join("k8-util/helm/fluvio-core"))
            .unwrap();

        let kube_options = KubeOptions {
            chart_location: Some(chart_dir.to_str().unwrap().to_owned()),
            ..Default::default()
        };

        let mut installer = ClusterInstaller::new().unwrap()
            .with_kube_options(kube_options);
        installer.develop = true;

        flv_future_aio::task::run_block_on(installer.install_fluvio());
    }
}
