use std::{
    fs::File,
    path::PathBuf,
    process::{Command, Stdio},
};
use tracing::info;

use fluvio_command::CommandExt;

use crate::pick_unused_port;

use super::LocalRuntimeError;

pub(crate) struct EtcdProcess {
    pub log_dir: PathBuf,
    pub data_dir: PathBuf,
    pub base: PathBuf,
}

impl EtcdProcess {
    pub fn start(&self) -> Result<String, LocalRuntimeError> {
        let outputs = File::create(format!("{}/flv_etcd.log", self.log_dir.display()))?;
        let errors = outputs.try_clone()?;

        let port = pick_unused_port()?;
        let endpoint = format!("http://127.0.0.1:{port}");

        let mut binary = {
            let mut cmd = Command::new(&self.base);
            cmd.arg("etcd")
                .arg("--data-dir")
                .arg(self.data_dir.join("etcd"))
                .arg("--listen-client-urls")
                .arg(&endpoint)
                .arg("--advertise-client-urls")
                .arg(&endpoint);
            cmd
        };
        info!(cmd = %binary.display(),"Invoking command");
        binary
            .stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()?;

        info!("succesfully launched etcd at {endpoint}");
        Ok(endpoint)
    }
}
