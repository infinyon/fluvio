use async_trait::async_trait;

use utils::bin::get_fluvio;

use crate::TestOption;
use crate::util::CommandUtil;

use super::EnvironmentDriver;

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
        get_fluvio()
            .expect("fluvio not founded")
            .arg("cluster")
            .arg("uninstall")
            .arg("--local")
            .inherit();
    }

    async fn install_cluster(&self) {
        let mut cmd = get_fluvio().expect("fluvio not founded");

        cmd.arg("cluster")
            .arg("install")
            .arg("--spu")
            .arg(self.option.spu.to_string())
            .arg("--local");

        if let Some(log) = &self.option.rust_log {
            cmd.arg("--rust-log").arg(log);
        }

        if let Some(log) = &self.option.log_dir {
            cmd.arg("--log-dir").arg(log);
        }

        if self.option.tls() {
            self.set_tls(&self.option, &mut cmd);
        }

        cmd.inherit();
    }
}
