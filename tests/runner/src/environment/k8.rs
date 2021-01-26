use async_trait::async_trait;

use fluvio_system_util::bin::get_fluvio;

use crate::TestOption;
use super::EnvironmentDriver;
use fluvio_command::CommandExt;

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
        let mut command = get_fluvio().expect("fluvio not founded");
        command.arg("cluster").arg("delete");
        println!("Executing> {}", command.display());
        command
            .inherit()
            .result()
            .expect("fluvio cluster delete should work");
    }

    async fn start_cluster(&self) {
        use std::time::Duration;
        use fluvio_future::timer::sleep;

        let mut cmd = get_fluvio().expect("fluvio not founded");

        cmd.arg("cluster")
            .arg("start")
            .arg("--spu")
            .arg(self.option.spu.to_string());

        if self.option.tls() {
            self.set_tls(&self.option, &mut cmd);
        }

        if let Some(ref authorization_config_map) = self.option.authorization_config_map {
            cmd.arg("--authorization-config-map")
                .arg(authorization_config_map);
        }

        if self.option.develop_mode() {
            cmd.arg("--develop");
        }

        if let Some(log) = &self.option.server_log {
            cmd.arg("--rust-log").arg(log);
        }

        if self.option.skip_checks() {
            cmd.arg("--skip-checks");
        }

        println!("Executing> {}", cmd.display());
        cmd.inherit()
            .result()
            .expect("fluvio cluster start should work");

        sleep(Duration::from_millis(2000)).await;
    }
}

/*
fn print_sc_logs() {
    use std::process::Command;

    let _ = Command::new("kubectl")
        .arg("logs")
        .arg("fluvio-sc")
        .print()
        .inherit();

    let _ = Command::new("kubectl")
        .arg("get")
        .arg("spu")
        .print()
        .inherit();
}
*/
