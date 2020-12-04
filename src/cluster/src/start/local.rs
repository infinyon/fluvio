use std::path::Path;
use std::borrow::Cow;
use std::fs::{File, create_dir_all};
use std::process::{Command, Stdio};
use std::time::Duration;
use fluvio::{FluvioConfig};

use tracing::{info, warn, debug, instrument};
use fluvio::config::{TlsPolicy, TlsConfig, TlsPaths, ConfigFile, Profile, LOCAL_PROFILE};
use fluvio::metadata::spg::SpuGroupSpec;
use flv_util::cmd::CommandExt;
use fluvio_future::timer::sleep;
use fluvio::metadata::spu::{SpuSpec, SpuType};
use fluvio::metadata::spu::IngressPort;
use fluvio::metadata::spu::Endpoint;
use fluvio::metadata::spu::IngressAddr;
use k8_obj_metadata::InputK8Obj;
use k8_obj_metadata::InputObjectMeta;
use k8_client::SharedK8Client;

use crate::{LocalInstallError, ClusterError, UnrecoverableCheck, StartStatus, DEFAULT_NAMESPACE};
use crate::check::{
    InstallCheck, HelmVersion, SysChart, RecoverableCheck, CheckResults, K8Version, LoadableConfig,
};
use crate::start::k8::ClusterInstaller;
use crate::start::check_and_fix;

const LOCAL_SC_ADDRESS: &str = "localhost:9003";
const DEFAULT_CHART_LOCATION: &str = "./k8-util/helm";

#[derive(Debug)]
pub struct LocalClusterInstallerBuilder {
    /// The directory where log files are
    log_dir: String,
    /// The logging settings to set in the cluster
    rust_log: Option<String>,
    /// SPU spec
    spu_spec: SpuGroupSpec,
    /// The TLS policy for the SC and SPU servers
    server_tls_policy: TlsPolicy,
    /// The TLS policy for the client
    client_tls_policy: TlsPolicy,
    /// install system charts automatically
    install_sys: bool,
    /// Should the pre install checks be skipped
    skip_checks: bool,
}

impl LocalClusterInstallerBuilder {
    /// Creates a `LocalClusterInstaller` with the current configuration.
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
    /// # use fluvio_cluster::LocalClusterInstaller;
    /// let installer = LocalClusterInstaller::new()
    ///     .build()
    ///     .expect("should create LocalClusterInstaller");
    /// ```
    ///
    pub fn build(self) -> Result<LocalClusterInstaller, ClusterError> {
        Ok(LocalClusterInstaller { config: self })
    }

    /// Sets the number of SPU replicas that should be provisioned. Defaults to 1.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::LocalClusterInstaller;
    /// let installer = LocalClusterInstaller::new()
    ///     .with_spu_replicas(2)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_spu_replicas(mut self, spu_replicas: u16) -> Self {
        self.spu_spec.replicas = spu_replicas;
        self
    }

    /// Sets the log directory.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::LocalClusterInstaller;
    /// let installer = LocalClusterInstaller::new()
    ///     .with_log_dir("/tmp".to_string())
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_log_dir(mut self, log_dir: String) -> Self {
        self.log_dir = log_dir;
        self
    }

    /// Sets the [`RUST_LOG`] environment variable for the installation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::LocalClusterInstaller;
    /// let installer = LocalClusterInstaller::new()
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
    /// use fluvio_cluster::LocalClusterInstaller;
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
    /// let installer = LocalClusterInstaller::new()
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
        self.client_tls_policy = client_policy;
        self.server_tls_policy = server_policy;
        self
    }
    /// Whether to install the `fluvio-sys` chart in the full installation. Defaults to `true`.
    ///
    /// # Example
    ///
    /// If you want to disable installing the system chart, you can do this
    ///
    /// ```no_run
    /// # use fluvio_cluster::LocalClusterInstaller;
    /// let installer = LocalClusterInstaller::new()
    ///     .with_system_chart(false)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_system_chart(mut self, install_sys: bool) -> Self {
        self.install_sys = install_sys;
        self
    }
    /// Whether to skip pre-install checks before installation. Defaults to `false`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use fluvio_cluster::LocalClusterInstaller;
    /// let installer = LocalClusterInstaller::new()
    ///     .with_skip_checks(true)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn with_skip_checks(mut self, skip_checks: bool) -> Self {
        self.skip_checks = skip_checks;
        self
    }
}

/// Install fluvio cluster locally
#[derive(Debug)]
pub struct LocalClusterInstaller {
    /// Configuration options for this process
    config: LocalClusterInstallerBuilder,
}

impl LocalClusterInstaller {
    /// Creates a default `LocalClusterInstallerBuilder` which can build a `LocalClusterInstaller`
    ///
    /// # Example
    ///
    /// The easiest way to build a `LocalClusterInstaller` is as follows:
    ///
    /// ```no_run
    /// use fluvio_cluster::LocalClusterInstaller;
    /// let installer = LocalClusterInstaller::new().build().unwrap();
    /// ```
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> LocalClusterInstallerBuilder {
        let spu_spec = SpuGroupSpec {
            replicas: 1,
            min_id: 0,
            ..SpuGroupSpec::default()
        };
        LocalClusterInstallerBuilder {
            spu_spec,
            rust_log: Some("info".to_string()),
            log_dir: "/tmp".to_string(),
            server_tls_policy: TlsPolicy::Disabled,
            client_tls_policy: TlsPolicy::Disabled,
            install_sys: true,
            skip_checks: false,
        }
    }

    /// Checks if all of the prerequisites for installing Fluvio locally are met
    /// and tries to auto-fix the issues observed
    pub async fn setup(&self) -> CheckResults {
        println!("Performing pre-flight checks");
        let checks: Vec<Box<dyn InstallCheck>> = vec![
            Box::new(HelmVersion),
            Box::new(K8Version),
            Box::new(LoadableConfig),
            Box::new(SysChart),
        ];
        let fix = |err| self.pre_install_fix(err);
        check_and_fix(&checks, fix).await
    }

    /// Given a pre-check error, attempt to automatically correct it
    #[instrument(skip(self, error))]
    async fn pre_install_fix(&self, error: RecoverableCheck) -> Result<(), UnrecoverableCheck> {
        // Depending on what error occurred, try to fix the error.
        // If we handle the error successfully, return Ok(()) to indicate success
        // If we cannot handle this error, return it to bubble up
        match error {
            RecoverableCheck::MissingSystemChart if self.config.install_sys => {
                debug!("Fluvio system chart not installed. Attempting to install");

                // Use closure to catch any errors
                let result = (|| -> Result<_, ClusterError> {
                    let installer = ClusterInstaller::new()
                        .with_namespace(DEFAULT_NAMESPACE)
                        .with_local_chart(DEFAULT_CHART_LOCATION)
                        .build()?;
                    installer._install_sys()?;
                    Ok(())
                })();

                // If any errors occurred, recovery failed
                if result.is_err() {
                    return Err(UnrecoverableCheck::FailedRecovery(error));
                }
            }
            unhandled => {
                warn!("Pre-install was unable to autofix an error");
                return Err(UnrecoverableCheck::FailedRecovery(unhandled));
            }
        }

        Ok(())
    }

    /// Install fluvio locally
    pub async fn install(&self) -> Result<StartStatus, ClusterError> {
        let checks = match self.config.skip_checks {
            true => None,
            false => {
                // Try to setup environment by running pre-checks and auto-fixes
                let check_results = self.setup().await;

                // If any check results encountered an error, bubble the error
                if check_results.0.iter().any(|it| it.is_err()) {
                    return Err(LocalInstallError::PrecheckErrored(check_results).into());
                }

                // If any checks successfully completed with a failure, return checks in status
                let statuses = check_results.into_statuses();
                let any_failed = statuses
                    .0
                    .iter()
                    .any(|it| matches!(it, crate::CheckStatus::Fail(_)));
                if any_failed {
                    return Err(LocalInstallError::FailedPrecheck(statuses).into());
                }

                Some(statuses)
            }
        };

        debug!("using log dir: {}", &self.config.log_dir);
        if !Path::new(&self.config.log_dir.to_string()).exists() {
            create_dir_all(&self.config.log_dir.to_string()).map_err(LocalInstallError::IoError)?;
        }
        // ensure we sync files before we launch servers
        Command::new("sync").inherit();
        info!("launching sc");
        let address = self.launch_sc()?;
        info!("setting local profile");
        self.set_profile()?;

        info!(
            "launching spu group with size: {}",
            &self.config.spu_spec.replicas
        );
        self.launch_spu_group().await?;
        sleep(Duration::from_secs(1)).await;
        self.confirm_spu(self.config.spu_spec.replicas).await?;

        Ok(StartStatus { address, checks })
    }

    /// Launches an SC on the local machine
    ///
    /// Returns the address of the SC if successful
    fn launch_sc(&self) -> Result<String, LocalInstallError> {
        let outputs = File::create(format!("{}/flv_sc.log", &self.config.log_dir))?;
        let errors = outputs.try_clone()?;
        debug!("starting sc server");
        let mut binary = {
            let mut cmd = Command::new(std::env::current_exe()?);
            cmd.arg("cluster").arg("run").arg("sc");
            cmd
        };
        if let TlsPolicy::Verified(tls) = &self.config.server_tls_policy {
            self.set_server_tls(&mut binary, tls, 9005)?;
        }
        if let Some(log) = &self.config.rust_log {
            binary.env("RUST_LOG", log);
        }
        let cmd = binary.print();
        cmd.stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()?;

        Ok(LOCAL_SC_ADDRESS.to_string())
    }

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
    fn set_profile(&self) -> Result<String, LocalInstallError> {
        let local_addr = LOCAL_SC_ADDRESS.to_owned();
        let mut config_file = ConfigFile::load_default_or_new()?;

        let config = config_file.mut_config();
        // check if local cluster exists otherwise, create new one
        match config.cluster_mut(LOCAL_PROFILE) {
            Some(cluster) => {
                cluster.addr = local_addr.clone();
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

    async fn launch_spu_group(&self) -> Result<(), LocalInstallError> {
        use k8_client::load_and_share;
        let client = load_and_share()?;
        let count = self.config.spu_spec.replicas;
        for i in 0..count {
            debug!("launching SPU ({} of {})", i + 1, count);
            self.launch_spu(i, client.clone(), &self.config.log_dir.to_string())
                .await?;
        }
        info!("SC log generated at {}/flv_sc.log", &self.config.log_dir);
        sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    async fn launch_spu(
        &self,
        spu_index: u16,
        client: SharedK8Client,
        log_dir: &str,
    ) -> Result<(), LocalInstallError> {
        use k8_client::metadata::MetadataClient;
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
        client.create_item(input).await?;
        // sleep 1 seconds for sc to connect
        sleep(Duration::from_millis(300)).await;
        let log_spu = format!("{}/spu_log_{}.log", log_dir, spu_id);
        let outputs = File::create(&log_spu)?;
        let errors = outputs.try_clone()?;

        let mut binary = {
            let mut cmd = Command::new(std::env::current_exe()?);
            cmd.arg("cluster").arg("run").arg("spu");
            cmd
        };

        if let TlsPolicy::Verified(tls) = &self.config.server_tls_policy {
            self.set_server_tls(&mut binary, tls, private_port + 1)?;
        }
        if let Some(log) = &self.config.rust_log {
            binary.env("RUST_LOG", log);
        }
        let cmd = binary
            .arg("-i")
            .arg(format!("{}", spu_id))
            .arg("-p")
            .arg(format!("0.0.0.0:{}", public_port))
            .arg("-v")
            .arg(format!("0.0.0.0:{}", private_port))
            .print();
        info!("SPU<{}> cmd: {:#?}", spu_index, cmd);
        info!("SPU log generated at {}", log_spu);
        cmd.stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()
            .map_err(|_| LocalInstallError::Other("SPU server failed to start".to_string()))?;
        Ok(())
    }

    /// Check to ensure SPUs are all running
    async fn confirm_spu(&self, spu: u16) -> Result<(), LocalInstallError> {
        use fluvio::Fluvio;

        let delay: u64 = std::env::var("FLV_SPU_DELAY")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .unwrap_or(1);

        debug!("waiting for spu to be provisioned for: {} seconds", delay);

        sleep(Duration::from_secs(delay)).await;

        let client = Fluvio::connect().await?;
        let mut admin = client.admin().await;

        // wait for list of spu
        for _ in 0..30u16 {
            let spus = admin.list::<SpuSpec, _>(vec![]).await.expect("no spu list");
            let live_spus = spus.iter().filter(|spu| spu.status.is_online()).count();
            if live_spus == spu as usize {
                info!("{} SPUs provisioned", spus.len());
                drop(client);
                sleep(Duration::from_millis(1)).await; // give destructor time to clean up properly
                return Ok(());
            } else {
                debug!("{} out of {} SPUs up, waiting 5 sec", live_spus, spu);
                sleep(Duration::from_secs(5)).await;
            }
        }

        println!("waited too long,bailing out");
        Err(LocalInstallError::Other(format!(
            "not able to provision:{} spu",
            spu
        )))
    }
}
