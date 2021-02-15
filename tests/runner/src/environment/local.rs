use async_trait::async_trait;

use crate::{TestOption, load_tls};

use super::EnvironmentDriver;
use fluvio_cluster::{ClusterUninstaller, LocalConfig, LocalInstaller};

/// Local Env driver where we should SPU locally
pub struct LocalEnvDriver {
    option: TestOption,
}

impl LocalEnvDriver {
    pub fn new(option: TestOption) -> Self {
        Self { option }
    }
}

#[async_trait]
impl EnvironmentDriver for LocalEnvDriver {
    /// remove cluster
    async fn remove_cluster(&self) {
        let uninstaller = ClusterUninstaller::new().build().unwrap();
        uninstaller.uninstall_local().await.unwrap();
    }

    async fn start_cluster(&self) {
        let mut builder = LocalConfig::builder(crate::VERSION);

        let fluvio_exe = std::env::current_exe()
            .ok()
            .and_then(|it| it.parent().map(|parent| parent.join("fluvio")));

        builder
            .spu_replicas(self.option.spu)
            .render_checks(true)
            .launcher(fluvio_exe);

        if let Some(rust_log) = &self.option.server_log {
            builder.rust_log(rust_log);
        }

        if let Some(log_dir) = &self.option.log_dir {
            builder.log_dir(log_dir);
        }

        if self.option.tls() {
            let (client, server) = load_tls(&self.option.tls_user);
            builder.tls(client, server);
        }

        let config = builder.build().expect("should build LocalConfig");
        let installer = LocalInstaller::from_config(config);
        installer.install().await.unwrap();
    }
}
