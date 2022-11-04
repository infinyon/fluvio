use async_trait::async_trait;

use fluvio_cluster::{ClusterConfig, ClusterInstaller, ClusterUninstallConfig, StartStatus};

use crate::tls::load_tls;
use crate::test_meta::environment::{EnvironmentSetup, EnvDetail};
use super::EnvironmentDriver;

#[derive(Clone)]
pub struct K8EnvironmentDriver {
    option: EnvironmentSetup,
}

impl K8EnvironmentDriver {
    pub fn new(option: EnvironmentSetup) -> Self {
        Self { option }
    }
}

#[async_trait]
impl EnvironmentDriver for K8EnvironmentDriver {
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
        let version = semver::Version::parse(&crate::VERSION).unwrap();
        let mut builder = ClusterConfig::builder(version);
        if self.option.develop_mode() {
            builder.development().expect("should test in develop mode");
        } else {
            // check if image version is specified
            if let Some(image) = &self.option.image_version {
                builder.image_tag(image);
            }
        }

        builder
            .proxy_addr(self.option.proxy_addr.clone())
            .spu_replicas(self.option.spu())
            .skip_checks(self.option.skip_checks())
            .save_profile(true)
            .hide_spinner(false);

        if self.option.tls {
            let (client, server) = load_tls(&self.option.tls_user());
            builder.tls(client, server);
        }

        if let Some(authorization_config_map) = &self.option.authorization_config_map() {
            builder.authorization_config_map(authorization_config_map);
        }

        if let Some(log) = &self.option.server_log {
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

    fn create_cluster_manager(&self) -> Box<dyn fluvio_cluster::runtime::spu::SpuClusterManager> {
        panic!("not yet implemented")
    }
}
