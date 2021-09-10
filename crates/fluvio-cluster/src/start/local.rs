use std::path::{Path, PathBuf};
use std::borrow::Cow;
use std::fs::{File, create_dir_all};
use std::process::{Command, Stdio};
use std::time::Duration;
use fluvio::{Fluvio, FluvioConfig};
use semver::Version;

use derive_builder::Builder;
use tracing::{info, warn, debug, instrument};
use once_cell::sync::Lazy;
use fluvio::config::{TlsPolicy, TlsConfig, TlsPaths, ConfigFile, Profile, LOCAL_PROFILE};
use fluvio_controlplane_metadata::spu::{SpuSpec, SpuType};
use fluvio::metadata::spu::IngressPort;
use fluvio::metadata::spu::Endpoint;
use fluvio::metadata::spu::IngressAddr;
use fluvio_future::timer::sleep;
use fluvio_command::CommandExt;
use k8_types::{InputK8Obj, InputObjectMeta};
use k8_client::SharedK8Client;

use crate::{
    CheckResult, ClusterChecker, ClusterError, K8InstallError, LocalInstallError, StartStatus,
    UserChartLocation,
};
use crate::charts::{ChartConfig};
use crate::check::{CheckResults, SysChartCheck};

use super::constants::*;
use super::common::check_crd;
use futures_channel::mpsc::{Receiver, Sender};
use futures_util::{StreamExt, SinkExt};

pub static DEFAULT_DATA_DIR: Lazy<Option<PathBuf>> =
    Lazy::new(|| directories::BaseDirs::new().map(|it| it.home_dir().join(".fluvio/data")));

const DEFAULT_LOG_DIR: &str = "/tmp";
const DEFAULT_RUST_LOG: &str = "info";
const DEFAULT_SPU_REPLICAS: u16 = 1;
const DEFAULT_TLS_POLICY: TlsPolicy = TlsPolicy::Disabled;
const LOCAL_SC_ADDRESS: &str = "localhost:9003";
const LOCAL_SC_PORT: u16 = 9003;

static DEFAULT_RUNNER_PATH: Lazy<Option<PathBuf>> = Lazy::new(|| std::env::current_exe().ok());

/// Describes how to install Fluvio locally
#[derive(Builder, Debug, Clone)]
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
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
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
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
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
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
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
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .spu_replicas(2)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "DEFAULT_SPU_REPLICAS")]
    spu_replicas: u16,
    /// The TLS policy for the SC and SPU servers
    #[builder(private, default = "DEFAULT_TLS_POLICY")]
    server_tls_policy: TlsPolicy,
    /// The TLS policy for the client
    #[builder(private, default = "DEFAULT_TLS_POLICY")]
    client_tls_policy: TlsPolicy,
    /// The version of the Fluvio system chart to install
    ///
    /// This is the only required field that does not have a default value.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
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

    /// Whether to install the `fluvio-sys` chart in the full installation.
    ///
    /// Defaults to `true`.
    ///
    /// # Example
    ///
    /// If you want to disable installing the system chart, you can do this
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .install_sys(false)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "true")]
    install_sys: bool,
    /// Whether to skip pre-install checks before installation.
    ///
    /// Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .skip_checks(false)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "false")]
    skip_checks: bool,
    /// Whether to render pre-install checks to stdout as they are performed.
    ///
    /// Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalConfigBuilder};
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .render_checks(true)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[builder(default = "false")]
    render_checks: bool,
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

    fn launcher_path(&self) -> Option<&Path> {
        self.launcher.as_deref()
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
    /// # fn example() -> Result<(), ClusterError> {
    /// use semver::Version;
    /// let config: LocalConfig = LocalConfig::builder(Version::parse("0.7.0-alpha.1").unwrap()).build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn build(&self) -> Result<LocalConfig, ClusterError> {
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
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
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
    pub fn tls<C: Into<TlsPolicy>, S: Into<TlsPolicy>>(
        &mut self,
        client: C,
        server: S,
    ) -> &mut Self {
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
    /// # fn example(builder: &mut LocalConfigBuilder) -> Result<(), ClusterError> {
    /// let config = builder
    ///     .local_chart("./k8-util/helm")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`with_remote_chart`]: ./struct.ClusterInstallerBuilder#method.with_remote_chart
    pub fn local_chart<S: Into<PathBuf>>(&mut self, local_chart_location: S) -> &mut Self {
        self.chart_location(UserChartLocation::Local(local_chart_location.into()));
        self
    }
}

#[derive(Debug)]
pub enum LocalSetupProgressMessage {
    PreFlighCheck(u64),
    Check(CheckResult),
}

/// Install fluvio cluster locally
#[derive(Debug, Clone)]
pub struct LocalInstaller {
    /// Configuration options for this process
    config: LocalConfig,
}

impl LocalInstaller {
    /// Creates a `LocalInstaller` with the given configuration options
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio_cluster::{ClusterError, LocalInstaller, LocalConfig};
    /// # fn example() -> Result<(), ClusterError> {
    /// use semver::Version;
    /// let config = LocalConfig::builder(Version::parse("0.7.0-alpha.1").unwrap()).build()?;
    /// let installer = LocalInstaller::from_config(config);
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_config(config: LocalConfig) -> Self {
        Self { config }
    }

    /// Checks if all of the prerequisites for installing Fluvio locally are met
    /// and tries to auto-fix the issues observed
    pub async fn setup(&self) -> CheckResults {
        let mut receiver = self.setup_with_progress().await;
        let mut check_results = CheckResults::default();

        while let Some(local_progress) = receiver.next().await {
            if let LocalSetupProgressMessage::Check(check_result) = local_progress {
                check_results.push(check_result);
            }
        }
        check_results
    }

    /// Checks if all of the prerequisites for installing Fluvio locally are met
    /// and tries to auto-fix the issues observed
    pub async fn setup_with_progress(&self) -> Receiver<LocalSetupProgressMessage> {
        let (sender, receiver) = futures_channel::mpsc::channel(100);
        fluvio_future::task::spawn(self.clone().setup_with_progress_impl(sender));
        receiver
    }

    async fn setup_with_progress_impl(
        self,
        mut sender: Sender<LocalSetupProgressMessage>,
    ) -> Result<(), ClusterError> {
        let mut sys_config: ChartConfig = ChartConfig::sys_builder()
            .version(self.config.chart_version.clone())
            .build()
            .expect("should build config since all required arguments are given");

        if let Some(location) = &self.config.chart_location {
            sys_config.location = location.clone().into();
        }

        let cluster_checker = ClusterChecker::empty()
            .with_local_checks()
            .with_check(SysChartCheck::new(sys_config));

        let checks_number = cluster_checker.checks_number() as u64;
        sender
            .send(LocalSetupProgressMessage::PreFlighCheck(checks_number))
            .await
            .map_err(LocalInstallError::ChannelSendError)?;

        let mut progress = cluster_checker
            .run_and_fix_with_progress()
            .map(|check| Ok(LocalSetupProgressMessage::Check(check)));
        sender
            .send_all(&mut progress)
            .await
            .map_err(LocalInstallError::ChannelSendError)?;
        Ok(())
    }

    /// Install fluvio locally
    #[instrument(skip(self))]
    pub async fn install(&self) -> Result<StartStatus, ClusterError> {
        let checks = match self.config.skip_checks {
            true => None,
            false => {
                // Try to setup environment by running pre-checks and auto-fixes
                let check_results = self.setup().await;

                // If any check results encountered an error, bubble the error
                if check_results.iter().any(|it| it.is_err()) {
                    return Err(LocalInstallError::PrecheckErrored(check_results).into());
                }

                // If any checks successfully completed with a failure, return checks in status
                let statuses: Vec<_> = check_results.into_iter().filter_map(|it| it.ok()).collect();

                let any_failed = statuses
                    .iter()
                    .any(|it| matches!(it, crate::CheckStatus::Fail(_)));
                if any_failed {
                    return Err(LocalInstallError::FailedPrecheck(statuses).into());
                }

                Some(statuses)
            }
        };

        use k8_client::load_and_share;
        let client = load_and_share().map_err(K8InstallError::from)?;

        // before we do let's try make sure SPU are installed.
        check_crd(client.clone())
            .await
            .map_err(K8InstallError::from)?;

        debug!("using log dir: {}", self.config.log_dir.display());
        if !self.config.log_dir.exists() {
            create_dir_all(&self.config.log_dir).map_err(LocalInstallError::IoError)?;
        }
        // ensure we sync files before we launch servers
        Command::new("sync")
            .inherit()
            .result()
            .map_err(|e| ClusterError::InstallLocal(e.into()))?;

        // set host name and port for SC
        // this should mirror K8
        let (address, port) = (LOCAL_SC_ADDRESS.to_owned(), LOCAL_SC_PORT);

        let fluvio = self.launch_sc(&address, port).await?;

        println!("Launching spu group with: {}", &self.config.spu_replicas);
        self.launch_spu_group(client.clone()).await?;

        self.confirm_spu(self.config.spu_replicas, &fluvio).await?;

        println!("Setting local profile");
        self.set_profile()?;

        Ok(StartStatus {
            address,
            port,
            checks,
        })
    }

    /// Launches an SC on the local machine
    ///
    /// Returns the address of the SC if successful
    #[instrument(skip(self))]
    async fn launch_sc(&self, host_name: &str, port: u16) -> Result<Fluvio, LocalInstallError> {
        use super::common::try_connect_to_sc;

        let outputs = File::create(format!("{}/flv_sc.log", self.config.log_dir.display()))?;
        let errors = outputs.try_clone()?;
        println!("Starting sc server");
        let mut binary = {
            let base = self
                .config
                .launcher_path()
                .ok_or(LocalInstallError::MissingFluvioRunner)?;
            let mut cmd = Command::new(base);
            cmd.arg("run").arg("sc").arg("--local");
            cmd
        };
        if let TlsPolicy::Verified(tls) = &self.config.server_tls_policy {
            self.set_server_tls(&mut binary, tls, 9005)?;
        }
        binary.env("RUST_LOG", &self.config.rust_log);

        binary
            .stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()?;

        // wait little bit to spin up SC
        sleep(Duration::from_secs(2)).await;

        // construct config to connect to SC
        let cluster_config =
            FluvioConfig::new(&(*LOCAL_SC_ADDRESS)).with_tls(self.config.client_tls_policy.clone());

        if let Some(fluvio) =
            try_connect_to_sc(&cluster_config, &self.config.platform_version).await
        {
            Ok(fluvio)
        } else {
            Err(LocalInstallError::SCServiceTimeout)
        }
    }

    #[instrument(skip(self, cmd, tls, port))]
    fn set_server_tls(
        &self,
        cmd: &mut Command,
        tls: &TlsConfig,
        port: u16,
    ) -> Result<(), LocalInstallError> {
        let paths: Cow<TlsPaths> = match tls {
            TlsConfig::Files(paths) => Cow::Borrowed(paths),
            TlsConfig::Inline(certs) => Cow::Owned(certs.try_into_temp_files()?),
        };

        info!("starting SC with TLS options");
        let ca_cert = paths
            .ca_cert
            .to_str()
            .ok_or_else(|| LocalInstallError::Other("ca_cert must be a valid path".to_string()))?;
        let server_cert = paths.cert.to_str().ok_or_else(|| {
            LocalInstallError::Other("server_cert must be a valid path".to_string())
        })?;
        let server_key = paths.key.to_str().ok_or_else(|| {
            LocalInstallError::Other("server_key must be a valid path".to_string())
        })?;
        cmd.arg("--tls")
            .arg("--enable-client-cert")
            .arg("--server-cert")
            .arg(server_cert)
            .arg("--server-key")
            .arg(server_key)
            .arg("--ca-cert")
            .arg(ca_cert)
            .arg("--bind-non-tls-public")
            .arg(format!("0.0.0.0:{}", port));
        Ok(())
    }

    /// set local profile
    #[instrument(skip(self))]
    fn set_profile(&self) -> Result<String, LocalInstallError> {
        let local_addr = LOCAL_SC_ADDRESS.to_owned();
        let mut config_file = ConfigFile::load_default_or_new()?;

        let config = config_file.mut_config();
        // check if local cluster exists otherwise, create new one
        match config.cluster_mut(LOCAL_PROFILE) {
            Some(cluster) => {
                cluster.endpoint = local_addr.clone();
                cluster.tls = self.config.client_tls_policy.clone();
            }
            None => {
                let mut local_cluster = FluvioConfig::new(local_addr.clone());
                local_cluster.tls = self.config.client_tls_policy.clone();
                config.add_cluster(local_cluster, LOCAL_PROFILE.to_owned());
            }
        };

        // check if we local profile exits otherwise, create new one, then set it's cluster
        match config.profile_mut(LOCAL_PROFILE) {
            Some(profile) => {
                profile.set_cluster(LOCAL_PROFILE.to_owned());
            }
            None => {
                let profile = Profile::new(LOCAL_PROFILE.to_owned());
                config.add_profile(profile, LOCAL_PROFILE.to_owned());
            }
        }

        // finally we set current profile to local
        assert!(config.set_current_profile(LOCAL_PROFILE));

        config_file.save()?;

        Ok(format!("local context is set to: {}", local_addr))
    }

    #[instrument(skip(self))]
    async fn launch_spu_group(&self, client: SharedK8Client) -> Result<(), LocalInstallError> {
        let count = self.config.spu_replicas;
        for i in 0..count {
            println!("Starting SPU ({} of {})", i + 1, count);
            self.launch_spu(i, client.clone(), &self.config.log_dir)
                .await?;
        }
        info!(
            "SC log generated at {}/flv_sc.log",
            &self.config.log_dir.display()
        );
        sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    #[instrument(skip(self, client, log_dir))]
    async fn launch_spu(
        &self,
        spu_index: u16,
        client: SharedK8Client,
        log_dir: &Path,
    ) -> Result<(), LocalInstallError> {
        use k8_client::meta_client::MetadataClient;
        const BASE_PORT: u16 = 9010;
        const BASE_SPU: u16 = 5001;
        let spu_id = (BASE_SPU + spu_index) as i32;
        let public_port = BASE_PORT + spu_index * 10;
        let private_port = public_port + 1;
        let spu_spec = SpuSpec {
            id: spu_id,
            spu_type: SpuType::Custom,
            public_endpoint: IngressPort {
                port: public_port,
                ingress: vec![IngressAddr {
                    hostname: Some("localhost".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            private_endpoint: Endpoint {
                port: private_port,
                host: "localhost".to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };
        let input = InputK8Obj::new(
            spu_spec,
            InputObjectMeta {
                name: format!("custom-spu-{}", spu_id),
                namespace: "default".to_owned(),
                ..Default::default()
            },
        );

        debug!(input=?input,"creating spu");
        client.create_item(input).await?;
        debug!("sleeping 1 sec");
        // sleep 1 seconds for sc to connect
        sleep(Duration::from_millis(1000)).await;
        let log_spu = format!("{}/spu_log_{}.log", log_dir.display(), spu_id);
        let outputs = File::create(&log_spu)?;
        let errors = outputs.try_clone()?;

        let mut binary = {
            let base = self
                .config
                .launcher_path()
                .ok_or(LocalInstallError::MissingFluvioRunner)?;
            let mut cmd = Command::new(base);
            cmd.arg("run").arg("spu");
            cmd
        };

        if let TlsPolicy::Verified(tls) = &self.config.server_tls_policy {
            self.set_server_tls(&mut binary, tls, private_port + 1)?;
        }
        binary.env("RUST_LOG", &self.config.rust_log);
        let cmd = binary
            .arg("-i")
            .arg(format!("{}", spu_id))
            .arg("-p")
            .arg(format!("0.0.0.0:{}", public_port))
            .arg("-v")
            .arg(format!("0.0.0.0:{}", private_port))
            .arg("--log-base-dir")
            .arg(&self.config.data_dir);
        debug!("Invoking command: \"{}\"", cmd.display());
        info!("SPU<{}> cmd: {:#?}", spu_index, cmd);
        info!("SPU log generated at {}", log_spu);
        cmd.stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()
            .map_err(|_| LocalInstallError::Other("SPU server failed to start".to_string()))?;
        Ok(())
    }

    /// Check to ensure SPUs are all running
    #[instrument(skip(self, client))]
    async fn confirm_spu(&self, spu: u16, client: &Fluvio) -> Result<(), LocalInstallError> {
        let admin = client.admin().await;

        // wait for list of spu
        for _ in 0..*MAX_SC_NETWORK_LOOP {
            let spus = admin.list::<SpuSpec, _>(vec![]).await?;
            let ready_spu = spus.iter().filter(|spu| spu.status.is_online()).count();
            if ready_spu == spu as usize {
                println!("All SPUs({}) are ready", spu);
                sleep(Duration::from_millis(1)).await; // give destructor time to clean up properly
                return Ok(());
            } else {
                println!(
                    "{} of {} spu are ready, sleeping 10 seconds...",
                    ready_spu, spu,
                );
                debug!("{} out of {} SPUs up, waiting 10 sec", ready_spu, spu);
                sleep(Duration::from_secs(10)).await;
            }
        }

        println!("waited too long,bailing out");
        Err(LocalInstallError::Other(format!(
            "not able to provision:{} spu",
            spu
        )))
    }
}
