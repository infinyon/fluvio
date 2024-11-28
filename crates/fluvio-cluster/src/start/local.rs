use std::path::{Path, PathBuf};
use std::fs::create_dir_all;
use std::process::Command;
use std::time::{Duration, SystemTime};

use anyhow::Result;
use colored::Colorize;
use semver::Version;
use derive_builder::Builder;
use serde::{Serialize, Deserialize};
use tracing::{debug, error, instrument, warn};
use once_cell::sync::Lazy;

use fluvio::{Fluvio, FluvioConfig};
use fluvio::config::{TlsPolicy, ConfigFile, LOCAL_PROFILE};
use fluvio_controlplane_metadata::spu::{SpuSpec, CustomSpuSpec};
use fluvio_future::timer::sleep;
use fluvio_command::CommandExt;
use fluvio_types::config_file::SaveLoadConfig;
use k8_types::{InputK8Obj, InputObjectMeta};
use k8_client::SharedK8Client;

use crate::render::{ProgressRenderedText, ProgressRenderer};
use crate::{ClusterChecker, LocalInstallError, StartStatus, UserChartLocation, InstallationType};
use crate::charts::ChartConfig;
use crate::check::{SysChartCheck, ClusterCheckError};
use crate::runtime::local::{LocalSpuProcessClusterManager, ScProcess, ScMode};
use crate::progress::{InstallProgressMessage, ProgressBarFactory};

use super::constants::MAX_PROVISION_TIME_SEC;
use super::common::check_crd;

pub static LOCAL_CONFIG_PATH: Lazy<Option<PathBuf>> =
    Lazy::new(|| directories::BaseDirs::new().map(|it| it.home_dir().join(".fluvio/local-config")));
pub static DEFAULT_DATA_DIR: Lazy<Option<PathBuf>> =
    Lazy::new(|| directories::BaseDirs::new().map(|it| it.home_dir().join(".fluvio/data")));
pub const DEFAULT_METADATA_SUB_DIR: &str = "metadata";

const DEFAULT_LOG_DIR: &str = "/tmp";
const DEFAULT_RUST_LOG: &str = "info";
const DEFAULT_SPU_REPLICAS: u16 = 1;
const DEFAULT_TLS_POLICY: TlsPolicy = TlsPolicy::Disabled;
const LOCAL_SC_ADDRESS: &str = "127.0.0.1:9003";
const LOCAL_SC_PORT: &str = "9003";

pub static DEFAULT_RUNNER_PATH: Lazy<Option<PathBuf>> = Lazy::new(|| std::env::current_exe().ok());

/// Describes how to install Fluvio locally
#[derive(Builder, Debug, Clone, Serialize, Deserialize)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct LocalConfig {
    /// Platform version
    #[builder(setter(into))]
    platform_version: Version,

    /// Sets the application log directory.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> anyhow::Result<()> {
    /// let config = builder
    ///     .log_dir("/tmp")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(setter(into), default = "PathBuf::from(DEFAULT_LOG_DIR)")]
    log_dir: PathBuf,
    /// Sets the data-log directory. This is where streaming data is stored.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> anyhow::Result<()> {
    /// let config = builder
    ///     .data_dir("/tmp/fluvio")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(setter(into))]
    data_dir: PathBuf,
    /// Internal API: Path to the executable for running `cluster run`
    ///
    /// This is necessary because when `fluvio-cluster` is linked into any
    /// binary other than the Fluvio CLI, it needs to know how to invoke
    /// the cluster components. This is currently used for testing.
    #[doc(hidden)]
    #[builder(setter(into), default = "(*DEFAULT_RUNNER_PATH).clone()")]
    launcher: Option<PathBuf>,
    /// Sets the [`RUST_LOG`] environment variable for the installation.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> anyhow::Result<()> {
    /// let config = builder
    ///     .rust_log("debug")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`RUST_LOG`]: https://docs.rs/tracing-subscriber/0.2.11/tracing_subscriber/filter/struct.EnvFilter.html
    #[builder(setter(into), default = "DEFAULT_RUST_LOG.to_string()")]
    rust_log: String,
    /// Sets the number of SPU replicas that should be provisioned. Defaults to 1.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> anyhow::Result<()> {
    /// let config = builder
    ///     .spu_replicas(2)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "DEFAULT_SPU_REPLICAS")]
    spu_replicas: u16,
    /// The TLS policy for the SC and SPU servers
    #[builder(default = "DEFAULT_TLS_POLICY")]
    server_tls_policy: TlsPolicy,
    /// The TLS policy for the client
    #[builder(private, default = "DEFAULT_TLS_POLICY")]
    client_tls_policy: TlsPolicy,
    #[builder(default = "LOCAL_SC_ADDRESS.to_string()")]
    sc_pub_addr: String,
    #[builder(setter(into), default)]
    sc_priv_addr: Option<String>,
    /// The version of the Fluvio system chart to install
    ///
    /// This is the only required field that does not have a default value.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> anyhow::Result<()> {
    /// use semver::Version;
    /// let config = builder
    ///     .chart_version(Version::parse("0.7.0-alpha.1").unwrap())
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(setter(into), default)]
    chart_version: Option<Version>,

    /// chart location of sys chart
    #[builder(setter(into, strip_option), default)]
    chart_location: Option<UserChartLocation>,

    /// Whether to skip pre-install checks before installation.
    ///
    /// Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> anyhow::Result<()> {
    /// let config = builder
    ///     .skip_checks(false)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "false")]
    skip_checks: bool,

    #[builder(default = "true")]
    hide_spinner: bool,

    installation_type: InstallationType,

    #[builder(default)]
    read_only_config: Option<PathBuf>,

    #[builder(default = "false")]
    save_profile: bool,
}

impl LocalConfig {
    /// Creates a new default [`LocalConfigBuilder`]
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::LocalConfig;
    /// use semver::Version;
    /// let mut builder = LocalConfig::builder(Version::parse("0.7.0-alpha.1").unwrap());
    /// ```
    pub fn builder(platform_version: Version) -> LocalConfigBuilder {
        let mut builder = LocalConfigBuilder::default();
        builder.platform_version(platform_version);

        if let Some(data_dir) = &*DEFAULT_DATA_DIR {
            builder.data_dir(data_dir);
        }
        builder
    }

    pub fn platform_version(&self) -> &Version {
        &self.platform_version
    }

    pub fn launcher_path(&self) -> Option<&Path> {
        self.launcher.as_deref()
    }

    pub fn as_spu_cluster_manager(&self) -> LocalSpuProcessClusterManager {
        LocalSpuProcessClusterManager {
            log_dir: self.log_dir.to_owned(),
            rust_log: self.rust_log.clone(),
            launcher: self.launcher.clone(),
            tls_policy: self.server_tls_policy.clone(),
            data_dir: self.data_dir.clone(),
        }
    }

    // Create a builder from the instance, so it could be used to create an evovled instance.
    pub fn evolve(self) -> LocalConfigBuilder {
        // This direct assignment style is used so the compiler can guarentee pairity between LocalConfig and the builder object
        LocalConfigBuilder {
            platform_version: Some(self.platform_version),
            log_dir: Some(self.log_dir),
            data_dir: Some(self.data_dir),
            launcher: Some(self.launcher),
            rust_log: Some(self.rust_log),
            spu_replicas: Some(self.spu_replicas),
            server_tls_policy: Some(self.server_tls_policy),
            client_tls_policy: Some(self.client_tls_policy),
            sc_pub_addr: Some(self.sc_pub_addr),
            sc_priv_addr: Some(self.sc_priv_addr),
            chart_version: Some(self.chart_version),
            chart_location: Some(self.chart_location),
            skip_checks: Some(self.skip_checks),
            hide_spinner: Some(self.hide_spinner),
            installation_type: Some(self.installation_type),
            read_only_config: Some(self.read_only_config),
            save_profile: Some(self.save_profile),
        }
    }
}

impl LocalConfigBuilder {
    /// Creates a `LocalConfig` with the current configuration.
    ///
    /// # Example
    ///
    /// The simplest flow to create a `ClusterConfig` looks like:
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfig};
    /// # fn example() -> anyhow::Result<()> {
    /// use semver::Version;
    /// let config: LocalConfig = LocalConfig::builder(Version::parse("0.7.0-alpha.1").unwrap()).build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn build(&self) -> Result<LocalConfig> {
        let config = self
            .build_impl()
            .map_err(|err| LocalInstallError::MissingRequiredConfig(err.to_string()))?;
        Ok(config)
    }

    /// Sets the TLS Policy that the client and server will use to communicate.
    ///
    /// By default, these are set to `TlsPolicy::Disabled`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{LocalConfig, LocalConfigBuilder, ClusterError};
    /// # fn example(builder: &mut LocalConfigBuilder) -> anyhow::Result<()> {
    /// use std::path::PathBuf;
    /// use fluvio::config::TlsPaths;
    /// use fluvio_cluster::LocalInstaller;
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
    /// let config = LocalConfig::builder(Version::parse("0.7.0-alpha.1").unwrap())
    ///     .tls(client, server)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls(&mut self, client: impl Into<TlsPolicy>, server: impl Into<TlsPolicy>) -> &mut Self {
        let client_policy = client.into();
        let server_policy = server.into();

        use std::mem::discriminant;
        match (&client_policy, &server_policy) {
            // If the two policies do not have the same variant, they are probably incompatible
            _ if discriminant(&client_policy) != discriminant(&server_policy) => {
                warn!("Client TLS policy type is different than the Server TLS policy type!");
            }
            // If the client and server domains do not match, give a warning
            (TlsPolicy::Verified(client), TlsPolicy::Verified(server))
                if client.domain() != server.domain() =>
            {
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
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> anyhow::Result<()> {
    /// let config = builder
    ///     .local_chart("./k8-util/helm")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`with_remote_chart`]: ./struct.ClusterInstallerBuilder#method.with_remote_chart
    pub fn local_chart(&mut self, local_chart_location: impl Into<PathBuf>) -> &mut Self {
        self.chart_location(UserChartLocation::Local(local_chart_location.into()));
        self
    }
}

/// Install fluvio cluster locally
#[derive(Debug)]
pub struct LocalInstaller {
    /// Configuration options for this process
    config: LocalConfig,
    pb_factory: ProgressBarFactory,
}

impl LocalInstaller {
    /// Creates a `LocalInstaller` with the given configuration options
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalInstaller, LocalConfig};
    /// # fn example() -> anyhow::Result<()> {
    /// use semver::Version;
    /// let config = LocalConfig::builder(Version::parse("0.7.0-alpha.1").unwrap()).build()?;
    /// let installer = LocalInstaller::from_config(config);
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_config(config: LocalConfig) -> Self {
        Self {
            pb_factory: ProgressBarFactory::new(config.hide_spinner),
            config,
        }
    }

    /// Checks if all of the prerequisites for installing Fluvio locally are met
    /// and tries to auto-fix the issues observed
    pub async fn preflight_check(&self, fix: bool) -> Result<()> {
        match &self.config.installation_type {
            InstallationType::Local | InstallationType::ReadOnly => {
                self.pb_factory
                    .println(InstallProgressMessage::PreFlightCheck.msg());

                ClusterChecker::empty()
                    .with_no_k8_checks()
                    .run(&self.pb_factory, fix)
                    .await?;

                Ok(())
            }
            InstallationType::LocalK8 => {
                let mut sys_config: ChartConfig = ChartConfig::sys_builder()
                    .version(self.config.chart_version.clone())
                    .build()
                    .expect("should build config since all required arguments are given");

                if let Some(location) = &self.config.chart_location {
                    sys_config.location = location.to_owned().into();
                }

                self.pb_factory
                    .println(InstallProgressMessage::PreFlightCheck.msg());
                ClusterChecker::empty()
                    .with_local_checks()
                    .with_check(SysChartCheck::new(
                        sys_config,
                        self.config.platform_version.clone(),
                    ))
                    .run(&self.pb_factory, fix)
                    .await?;
                Ok(())
            }
            other => Err(ClusterCheckError::Other(format!(
                "Installation type {other} is not supported for local clusters"
            ))
            .into()),
        }
    }

    /// Install fluvio locally
    #[instrument(skip(self))]
    pub async fn install(&self) -> Result<StartStatus> {
        if !self.config.skip_checks {
            self.preflight_check(true).await?;
        };

        self.install_only().await
    }

    #[instrument(skip(self))]
    pub async fn install_only(&self) -> Result<StartStatus> {
        let pb = self.pb_factory.create()?;

        debug!("using log dir: {}", self.config.log_dir.display());
        pb.set_message("Creating log directory");
        if !self.config.log_dir.exists() {
            create_dir_all(&self.config.log_dir).map_err(|e| {
                LocalInstallError::LogDirectoryError {
                    path: self.config.log_dir.clone(),
                    source: e,
                }
            })?;
        }

        let maybe_k8_client = if let InstallationType::LocalK8 = self.config.installation_type {
            use k8_client::load_and_share;
            Some(load_and_share()?)
        } else {
            None
        };

        if let Some(ref client) = maybe_k8_client {
            pb.set_message("Ensure CRDs are installed");
            // before we do let's try make sure SPU are installed.
            check_crd(client.clone()).await?;
            pb.set_message("CRD Checked");
        }

        pb.set_message("Sync files");
        // ensure we sync files before we launch servers
        Command::new("sync")
            .inherit()
            .result()
            .map_err(|e| LocalInstallError::Other(format!("sync issue: {e:#?}")))?;

        pb.println(format!("{} {}", "✅".bold(), "Local Cluster initialized"));
        pb.finish_and_clear();
        drop(pb);

        if self.config.save_profile {
            self.set_profile()?;
        }

        let pb = self.pb_factory.create()?;
        let fluvio = self
            .launch_sc(
                self.config.sc_pub_addr.clone(),
                self.config.sc_priv_addr.clone(),
                &pb,
            )
            .await?;
        pb.println(InstallProgressMessage::ScLaunched.msg());
        pb.finish_and_clear();

        let pb: ProgressRenderer = self.pb_factory.create()?;

        if let Some(ref client) = maybe_k8_client {
            self.launch_spu_group(client.clone(), &pb).await?;
        } else {
            self.launch_spu_group_read_only(&fluvio, &pb).await?;
        }

        self.confirm_spu(self.config.spu_replicas, &fluvio, &pb)
            .await?;
        pb.println(format!("✅ {} SPU launched", self.config.spu_replicas));
        pb.finish_and_clear();
        drop(pb);

        self.pb_factory
            .println("🎯 Successfully installed Local Fluvio cluster");

        self.save_config_file();

        let port: u16 = self
            .config
            .sc_pub_addr
            .split(':')
            .last()
            .unwrap_or(LOCAL_SC_PORT)
            .parse()?;

        Ok(StartStatus {
            address: self.config.sc_pub_addr.clone(),
            port,
        })
    }

    /// Launches an SC on the local machine
    ///
    /// Returns the address of the SC if successful
    #[instrument(skip(self))]
    async fn launch_sc(
        &self,
        public_address: String,
        private_address: Option<String>,
        pb: &ProgressRenderer,
    ) -> Result<Fluvio> {
        use super::common::try_connect_to_sc;

        pb.set_message(InstallProgressMessage::LaunchingSC.msg());

        let mode = match &self.config.installation_type {
            InstallationType::Local => {
                ScMode::Local(self.config.data_dir.join(DEFAULT_METADATA_SUB_DIR))
            }
            InstallationType::LocalK8 | InstallationType::K8 => ScMode::K8s,
            InstallationType::ReadOnly => {
                ScMode::ReadOnly(self.config.read_only_config.clone().unwrap_or_default())
            }
            other => {
                return Err(LocalInstallError::Other(format!(
                    "Installation type {other} is not supported for local clusters"
                ))
                .into())
            }
        };

        let sc_process = ScProcess {
            log_dir: self.config.log_dir.clone(),
            launcher: self.config.launcher.clone(),
            tls_policy: self.config.server_tls_policy.clone(),
            rust_log: self.config.rust_log.clone(),
            mode,
            private_address,
            public_address: public_address.clone(),
        };

        sc_process.start()?;

        // wait little bit to spin up SC
        sleep(Duration::from_secs(2)).await;

        // construct config to connect to SC
        let cluster_config =
            FluvioConfig::new(public_address).with_tls(self.config.client_tls_policy.clone());

        try_connect_to_sc(&cluster_config, &self.config.platform_version, pb)
            .await
            .ok_or(LocalInstallError::SCServiceTimeout.into())
    }

    /// set local profile
    #[instrument(skip(self))]
    fn set_profile(&self) -> Result<()> {
        let pb = self.pb_factory.create()?;
        pb.set_message(format!(
            "Creating Local Profile to: {}",
            self.config.sc_pub_addr
        ));

        let mut config_file = ConfigFile::load_default_or_new()?;
        config_file.add_or_replace_profile(
            LOCAL_PROFILE,
            &self.config.sc_pub_addr,
            &self.config.client_tls_policy,
        )?;
        let config = config_file.mut_config().current_cluster_mut()?;
        self.config.installation_type.save_to(config)?;
        config_file.save()?;

        pb.println(InstallProgressMessage::ProfileSet.msg());

        pb.finish_and_clear();

        Ok(())
    }

    #[instrument(skip(self))]
    async fn launch_spu_group(&self, client: SharedK8Client, pb: &ProgressRenderer) -> Result<()> {
        let count = self.config.spu_replicas;

        let runtime = self.config.as_spu_cluster_manager();
        for i in 0..count {
            pb.set_message(InstallProgressMessage::StartSPU(i + 1, count).msg());
            self.launch_spu(i, &runtime, client.clone()).await?;
        }
        debug!(
            "SC log generated at {}/flv_sc.log",
            &self.config.log_dir.display()
        );
        sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    #[instrument(skip(self, cluster_manager, client))]
    async fn launch_spu(
        &self,
        spu_index: u16,
        cluster_manager: &LocalSpuProcessClusterManager,
        client: SharedK8Client,
    ) -> Result<()> {
        use k8_client::meta_client::MetadataClient;
        use crate::runtime::spu::SpuClusterManager;

        let spu_process = cluster_manager.create_spu_relative(spu_index);

        let input = InputK8Obj::new(
            spu_process.spec().clone(),
            InputObjectMeta {
                name: format!("custom-spu-{}", spu_process.id()),
                namespace: "default".to_owned(),
                ..Default::default()
            },
        );

        debug!(input=?input,"creating spu");
        client.create_item(input).await?;
        debug!("sleeping 1 sec");
        // sleep 1 seconds for sc to connect
        sleep(Duration::from_millis(1000)).await;

        spu_process.start()
    }

    #[instrument(skip(self, fluvio))]
    async fn launch_spu_group_read_only(
        &self,
        fluvio: &Fluvio,
        pb: &ProgressRenderer,
    ) -> Result<()> {
        let count = self.config.spu_replicas;

        let runtime = self.config.as_spu_cluster_manager();

        for i in 0..count {
            pb.set_message(InstallProgressMessage::StartSPU(i + 1, count).msg());
            self.launch_spu_read_only(fluvio, i, &runtime).await?;
        }
        sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    /// Register and launch spu
    #[instrument(skip(self, cluster_manager, fluvio))]
    async fn launch_spu_read_only(
        &self,
        fluvio: &Fluvio,
        spu_index: u16,
        cluster_manager: &LocalSpuProcessClusterManager,
    ) -> Result<()> {
        use crate::runtime::spu::SpuClusterManager;

        let spu_process = cluster_manager.create_spu_relative(spu_index);
        let spec = spu_process.spec();
        let admin = fluvio.admin().await;
        let name = format!("custom-spu-{}", spu_process.id());
        if admin
            .list::<CustomSpuSpec, _>(vec![name.clone()])
            .await?
            .is_empty()
        {
            debug!(name, "create custom spu");
            admin
                .create::<CustomSpuSpec>(name, false, spec.to_owned().into())
                .await?;
        } else {
            debug!(name, "custom spu already exists");
        }
        spu_process.start()
    }

    /// Check to ensure SPUs are all running
    #[instrument(skip(self, client))]
    async fn confirm_spu(&self, spu: u16, client: &Fluvio, pb: &ProgressRenderer) -> Result<()> {
        let admin = client.admin().await;

        let pb = self.pb_factory.create()?;
        let timeout_duration = Duration::from_secs(*MAX_PROVISION_TIME_SEC as u64);
        let time = SystemTime::now();
        pb.set_message(format!(
            "🖥️ Waiting for SPUs to be ready and have ingress... (timeout: {timeout}s)",
            timeout = *MAX_PROVISION_TIME_SEC
        ));
        // wait for list of spu
        while time.elapsed().unwrap() < timeout_duration {
            let spus = admin.all::<SpuSpec>().await?;
            let ready_spu = spus.iter().filter(|spu| spu.status.is_online()).count();
            let elapsed = time.elapsed().unwrap();
            pb.set_message(format!(
                "🖥️ {}/{} SPU confirmed, {} seconds elapsed",
                ready_spu,
                spu,
                elapsed.as_secs()
            ));
            if ready_spu == spu as usize {
                sleep(Duration::from_millis(1000)).await; // give destructor time to clean up properly
                return Ok(());
            } else {
                debug!("{} out of {} SPUs up, waiting 10 sec", ready_spu, spu);
                sleep(Duration::from_secs(5)).await;
            }
        }

        error!(
            "waited too long: {} secs, bailing out",
            time.elapsed().unwrap().as_secs()
        );
        Err(LocalInstallError::Other(format!(
            "not able to provision:{spu} spu in {} secs",
            time.elapsed().unwrap().as_secs()
        ))
        .into())
    }

    fn save_config_file(&self) {
        let save_to_res = match LOCAL_CONFIG_PATH.as_ref() {
            None => {
                warn!("Local config path");
                return;
            }
            Some(local_config_path) => self.config.save_to(local_config_path),
        };

        if let Err(err) = save_to_res {
            warn!("Save error: {:?}", err);
        }
    }
}
