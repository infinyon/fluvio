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

        use std::time::Duration;
        use flv_future_aio::timer::sleep;

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


        sleep(Duration::from_millis(2000)).await;

        // display sc pod
        print_sc_logs();
    }
}

fn print_sc_logs() {
    use std::process::Command;

    let _ = Command::new("kubectl")
        .arg("logs")
        .arg("flv-sc")
        .print()
        .inherit();
}
