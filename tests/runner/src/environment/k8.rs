use async_trait::async_trait;

use crate::{TestOption, load_tls};
use super::EnvironmentDriver;
use fluvio_cluster::{ClusterUninstaller, ClusterConfig, ClusterInstaller};

pub struct K8EnvironmentDriver {
    option: TestOption,
}

impl K8EnvironmentDriver {
    pub fn new(option: TestOption) -> Self {
        Self { option }
    }
}

#[async_trait]
impl EnvironmentDriver for K8EnvironmentDriver {
    /// remove cluster
    async fn remove_cluster(&self) {
        let uninstaller = ClusterUninstaller::new().build().unwrap();
        uninstaller.uninstall().await.unwrap();
    }

    async fn start_cluster(&self) {
        let mut builder = ClusterConfig::builder(crate::VERSION);
        if self.option.develop_mode() {
            builder.development().expect("should test in develop mode");
        }
        builder
            .spu_replicas(self.option.spu)
            .skip_checks(self.option.skip_checks())
            .save_profile(true);

        if self.option.tls() {
            let (client, server) = load_tls(&self.option.tls_user);
            builder.tls(client, server);
        }

        if let Some(authorization_config_map) = &self.option.authorization_config_map {
            builder.authorization_config_map(authorization_config_map);
        }

        if let Some(log) = &self.option.server_log {
            builder.rust_log(log);
        }

        let config = builder.build().unwrap();
        let installer = ClusterInstaller::from_config(config).unwrap();
        installer.install_fluvio().await.unwrap();
    }
}
