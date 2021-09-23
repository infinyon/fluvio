use std::borrow::Cow;
use std::process::{Command, Stdio};
use std::{fs::File, path::PathBuf};

use fluvio_controlplane_metadata::spu::SpuSpec;
use tracing::{debug, info, instrument};

use fluvio_command::CommandExt;
use fluvio::config::{TlsConfig, TlsPaths, TlsPolicy};

use crate::{LocalInstallError};

/// Process representing SPU
#[derive(Debug, Default)]
pub struct SpuProcess {
    pub id: u16,
    pub log_dir: PathBuf,
    pub spec: SpuSpec,
    pub launcher: Option<PathBuf>,
    pub rust_log: String,
    pub data_dir: PathBuf,
    pub tls_policy: TlsPolicy,
}

impl SpuProcess {
    #[instrument(skip(self))]
    pub fn start(&self) -> Result<(), LocalInstallError> {
        let log_spu = format!("{}/spu_log_{}.log", self.log_dir.display(), self.id);
        let outputs = File::create(&log_spu)?;
        let errors = outputs.try_clone()?;

        let launcher = self.launcher.clone();
        let mut binary = {
            let base = launcher.ok_or(LocalInstallError::MissingFluvioRunner)?;
            let mut cmd = Command::new(base);
            cmd.arg("run").arg("spu");
            cmd
        };

        if let TlsPolicy::Verified(tls) = &self.tls_policy {
            self.set_server_tls(&mut binary, tls, self.spec.private_endpoint.port + 1)?;
        }
        binary.env("RUST_LOG", &self.rust_log);
        let cmd = binary
            .arg("-i")
            .arg(format!("{}", self.id))
            .arg("-p")
            .arg(format!("0.0.0.0:{}", self.spec.public_endpoint.port))
            .arg("-v")
            .arg(format!("0.0.0.0:{}", self.spec.private_endpoint.port))
            .arg("--log-base-dir")
            .arg(&self.data_dir);
        debug!("Invoking command: \"{}\"", cmd.display());
        info!("SPU<{}> cmd: {:#?}", self.id, cmd);
        info!("SPU log generated at {}", log_spu);
        cmd.stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()
            .map_err(|_| LocalInstallError::Other("SPU server failed to start".to_string()))?;
        Ok(())
    }

    #[instrument(skip(self, cmd, tls, port))]
    fn set_server_tls(
        &self,
        cmd: &mut Command,
        tls: &TlsConfig,
        port: u16,
    ) -> Result<(), LocalInstallError> {
        let paths: Cow<TlsPaths> = match tls {
            TlsConfig::Files(paths) => Cow::Borrowed(paths),
            TlsConfig::Inline(certs) => Cow::Owned(certs.try_into_temp_files()?),
        };

        info!("starting SC with TLS options");
        let ca_cert = paths
            .ca_cert
            .to_str()
            .ok_or_else(|| LocalInstallError::Other("ca_cert must be a valid path".to_string()))?;
        let server_cert = paths.cert.to_str().ok_or_else(|| {
            LocalInstallError::Other("server_cert must be a valid path".to_string())
        })?;
        let server_key = paths.key.to_str().ok_or_else(|| {
            LocalInstallError::Other("server_key must be a valid path".to_string())
        })?;
        cmd.arg("--tls")
            .arg("--enable-client-cert")
            .arg("--server-cert")
            .arg(server_cert)
            .arg("--server-key")
            .arg(server_key)
            .arg("--ca-cert")
            .arg(ca_cert)
            .arg("--bind-non-tls-public")
            .arg(format!("0.0.0.0:{}", port));
        Ok(())
    }
}
