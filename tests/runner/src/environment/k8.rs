use async_trait::async_trait;

use utils::bin::get_fluvio;

use crate::TestOption;
use crate::util::CommandUtil;
use super::EnvironmentDriver;

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
        get_fluvio()
            .expect("fluvio not founded")
            .arg("cluster")
            .arg("uninstall")
            .print()
            .inherit();
    }

    async fn install_cluster(&self) {
        let mut cmd = get_fluvio().expect("fluvio not founded");

        cmd.arg("cluster")
            .arg("install")
            .arg("--spu")
            .arg(self.option.spu.to_string());

        if self.option.tls() {
            self.set_tls(&self.option, &mut cmd);
        }

        if self.option.develop_mode() {
            cmd.arg("--develop");
        }

        cmd.print().inherit();
    }
}
