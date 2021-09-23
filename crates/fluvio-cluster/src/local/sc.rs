use std::{
    fs::File,
    path::PathBuf,
    process::{Command, Stdio},
};

use fluvio::config::TlsPolicy;

use crate::LocalInstallError;

use super::FluvioProcess;

pub struct ScProcess {
    pub log_dir: PathBuf,
    pub launcher: Option<PathBuf>,
    pub tls_policy: TlsPolicy,
    pub rust_log: String,
}

impl FluvioProcess for ScProcess {}

impl ScProcess {
    pub fn start(&self) -> Result<(), LocalInstallError> {
        let outputs = File::create(format!("{}/flv_sc.log", self.log_dir.display()))?;
        let errors = outputs.try_clone()?;

        let launcher = self.launcher.clone();
        let mut binary = {
            let base = launcher.ok_or(LocalInstallError::MissingFluvioRunner)?;
            let mut cmd = Command::new(base);
            cmd.arg("run").arg("sc").arg("--local");
            cmd
        };
        if let TlsPolicy::Verified(tls) = &self.tls_policy {
            self.set_server_tls(&mut binary, tls, 9005)?;
        }
        binary.env("RUST_LOG", &self.rust_log);

        binary
            .stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()?;

        Ok(())
    }
}
