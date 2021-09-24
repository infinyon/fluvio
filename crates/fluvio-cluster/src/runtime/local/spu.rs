use std::process::{Command, Stdio};
use std::{fs::File, path::PathBuf};

use fluvio_controlplane_metadata::spu::{Endpoint, IngressAddr, IngressPort, SpuSpec, SpuType};
use tracing::{debug, info, instrument};

use fluvio_command::CommandExt;
use fluvio::config::{TlsPolicy};

use crate::{LocalConfig, LocalInstallError};

use super::FluvioLocalProcess;

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

impl FluvioLocalProcess for SpuProcess {}

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
}



const BASE_PORT: u16 = 9010;
const BASE_SPU: u16 = 5001;
/// manage spu process cluster
use crate::runtime::spu::SpuClusterManager;

pub struct LocalSpuProcessClusterManager(LocalConfig);

impl SpuClusterManager for LocalSpuProcessClusterManager {


    fn start_spu(&self, id: u16) {

        let spu_index = id - BASE_SPU;
        let public_port = BASE_PORT + spu_index * 10;
        let private_port = public_port + 1;
        let spu_spec = SpuSpec {
            id: id as i32,
            spu_type: SpuType::Custom,
            public_endpoint: IngressPort {
                port: public_port,
                ingress: vec![IngressAddr {
                    hostname: Some("localhost".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            private_endpoint: Endpoint {
                port: private_port,
                host: "localhost".to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        let process = SpuProcess {
            id: spu_spec.id as u16,
            spec: spu_spec,
            log_dir: self.0.log_dir.to_owned(),
            rust_log: self.0.rust_log.clone(),
            launcher: self.0.launcher.clone(),
            tls_policy: self.0.server_tls_policy.clone(),
            data_dir: self.0.data_dir.clone(),
        };

    }

    fn terminate_spu(id: u16) {
        todo!()
    }
}
