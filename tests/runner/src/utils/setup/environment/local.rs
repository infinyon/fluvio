use async_trait::async_trait;

use crate::tls::load_tls;
use crate::test_meta::environment::{EnvironmentSetup, EnvDetail};

use super::TestEnvironmentDriver;
use fluvio_cluster::{ClusterUninstaller, LocalConfig, LocalInstaller, StartStatus};

/// Local Env driver where we should SPU locally
pub struct LocalEnvDriver {
    option: EnvironmentSetup,
}

impl LocalEnvDriver {
    pub fn new(option: EnvironmentSetup) -> Self {
        Self { option }
    }
}

#[async_trait]
impl TestEnvironmentDriver for LocalEnvDriver {
    /// remove cluster
    async fn remove_cluster(&self) {
        let uninstaller = ClusterUninstaller::new().build().unwrap();
        uninstaller.uninstall_local().await.unwrap();
    }

    async fn start_cluster(&self) -> StartStatus {
        let version = semver::Version::parse(&*crate::VERSION).unwrap();
        let mut builder = LocalConfig::builder(version);
        builder.spu_replicas(self.option.spu()).render_checks(true);

        // Make sure to use the test build of 'fluvio' for cluster components
        let test_exe = std::env::current_exe();
        let build_dir = test_exe
            .ok()
            .and_then(|it| it.parent().map(|it| it.join("fluvio")));
        if let Some(path) = build_dir {
            builder.launcher(path);
        }

        if let Some(rust_log) = &self.option.server_log() {
            builder.rust_log(rust_log);
        }

        if let Some(log_dir) = &self.option.log_dir() {
            builder.log_dir(log_dir);
        }


        if self.option.tls {
            let (client, server) = load_tls(&self.option.tls_user());
            builder.tls(client, server);
        }

        let config = builder.build().expect("should build LocalConfig");
        let installer = LocalInstaller::from_config(config);
        installer
            .install()
            .await
            .expect("Failed to install local cluster")
    }
}
