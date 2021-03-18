use async_trait::async_trait;

use crate::{test_meta::TestCase, tls::load_tls};
use super::TestEnvironmentDriver;
use fluvio_cluster::{ClusterConfig, ClusterInstaller, ClusterUninstaller, StartStatus};

pub struct K8EnvironmentDriver {
    option: TestCase,
}

impl K8EnvironmentDriver {
    pub fn new(option: TestCase) -> Self {
        Self { option }
    }
}

#[async_trait]
impl TestEnvironmentDriver for K8EnvironmentDriver {
    /// remove cluster
    async fn remove_cluster(&self) {
        let uninstaller = ClusterUninstaller::new().build().unwrap();
        uninstaller.uninstall().await.unwrap();
    }

    async fn start_cluster(&self) -> StartStatus {
        let mut builder = ClusterConfig::builder(crate::VERSION);
        if self.option.develop_mode() {
            builder.development().expect("should test in develop mode");
        }
        builder
            .spu_replicas(self.option.environment.spu)
            .skip_checks(self.option.environment.skip_checks)
            .save_profile(true);

        if self.option.environment.tls {
            let (client, server) = load_tls(&self.option.environment.tls_user);
            builder.tls(client, server);
        }

        if let Some(authorization_config_map) = &self.option.environment.authorization_config_map {
            builder.authorization_config_map(authorization_config_map);
        }

        if let Some(log) = &self.option.environment.server_log {
            builder.rust_log(log);
        }

        let config = builder.build().unwrap();
        let installer =
            ClusterInstaller::from_config(config).expect("Could not create ClusterInstaller");
        installer
            .install_fluvio()
            .await
            .expect("Failed to install k8 cluster")
    }
}
