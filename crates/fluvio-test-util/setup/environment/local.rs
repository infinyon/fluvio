use async_trait::async_trait;

use crate::tls::load_tls;
use crate::test_meta::environment::{EnvironmentSetup, EnvDetail};
use fluvio_cluster::{LocalConfig, LocalInstaller, StartStatus, ClusterUninstallConfig};

use super::EnvironmentDriver;

/// Local Env driver where we should SPU locally
#[derive(Clone)]
pub struct LocalEnvDriver {
    config: LocalConfig,
}

impl LocalEnvDriver {
    pub fn new(option: EnvironmentSetup) -> Self {
        Self {
            config: Self::load_config(&option),
        }
    }

    fn load_config(option: &EnvironmentSetup) -> LocalConfig {
        let version = semver::Version::parse(&crate::VERSION).unwrap();
        let mut builder = LocalConfig::builder(version);
        builder.spu_replicas(option.spu()).hide_spinner(false);

        // Make sure to use the test build of 'fluvio' for cluster components
        let test_exe = std::env::current_exe();
        let build_dir = test_exe
            .ok()
            .and_then(|it| it.parent().map(|it| it.join("fluvio")));
        if let Some(path) = build_dir {
            builder.launcher(path);
        }

        if let Some(rust_log) = &option.server_log() {
            builder.rust_log(rust_log);
        }

        if let Some(log_dir) = &option.log_dir() {
            builder.log_dir(log_dir);
        }

        if option.tls {
            let (client, server) = load_tls(&option.tls_user());
            builder.tls(client, server);
        }

        builder.build().expect("should build LocalConfig")
    }
}

#[async_trait]
impl EnvironmentDriver for LocalEnvDriver {
    /// remove cluster
    async fn remove_cluster(&self) {
        let uninstaller = ClusterUninstallConfig::builder()
            .build()
            .expect("uninstall builder")
            .uninstaller()
            .expect("uninstaller");
        uninstaller.uninstall().await.expect("uninstall");
    }

    async fn start_cluster(&self) -> StartStatus {
        let installer = LocalInstaller::from_config(self.config.clone());
        installer
            .install()
            .await
            .expect("Failed to install local cluster")
    }

    fn create_cluster_manager(&self) -> Box<dyn fluvio_cluster::runtime::spu::SpuClusterManager> {
        Box::new(self.config.as_spu_cluster_manager())
    }
}
