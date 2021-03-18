use async_trait::async_trait;

use crate::{test_meta::TestCase, tls::load_tls};

use super::TestEnvironmentDriver;
use fluvio_cluster::{ClusterUninstaller, LocalConfig, LocalInstaller, StartStatus};

/// Local Env driver where we should SPU locally
pub struct LocalEnvDriver {
    option: TestCase,
}

impl LocalEnvDriver {
    pub fn new(option: TestCase) -> Self {
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
        let mut builder = LocalConfig::builder(crate::VERSION);

        // FIXME: Validate that this exists and throw a useful error message
        let fluvio_exe = std::env::current_exe()
            .ok()
            .and_then(|it| it.parent().map(|parent| parent.join("fluvio")));

        builder
            .spu_replicas(self.option.environment.spu)
            .render_checks(true)
            .launcher(fluvio_exe);

        if let Some(rust_log) = &self.option.environment.server_log {
            builder.rust_log(rust_log);
        }

        if let Some(log_dir) = &self.option.environment.log_dir {
            builder.log_dir(log_dir);
        }

        if self.option.environment.tls {
            let (client, server) = load_tls(&self.option.environment.tls_user);
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
