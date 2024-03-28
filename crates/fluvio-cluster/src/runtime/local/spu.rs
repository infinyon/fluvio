use std::process::{Command, Stdio};
use std::{fs::File, path::PathBuf};

use anyhow::{Result as AnyResult, anyhow};
use tracing::{debug, info, instrument};

use fluvio_types::defaults::SPU_PUBLIC_PORT;
use fluvio_controlplane_metadata::spu::{Endpoint, IngressAddr, IngressPort, SpuSpec, SpuType};

use fluvio_command::CommandExt;
use fluvio::config::TlsPolicy;
use fluvio_types::SpuId;

use crate::runtime::spu::{SpuClusterManager, SpuTarget};

use super::{FluvioLocalProcess, LocalRuntimeError};

/// Process representing SPU
#[derive(Debug, Default)]
pub struct LocalSpuProcess {
    pub id: i32,
    pub log_dir: String,
    pub spec: SpuSpec,
    pub launcher: Option<PathBuf>,
    pub rust_log: String,
    pub data_dir: PathBuf,
    pub tls_policy: TlsPolicy,
}

impl FluvioLocalProcess for LocalSpuProcess {}

impl SpuTarget for LocalSpuProcess {
    #[instrument(skip(self))]
    fn start(&self) -> AnyResult<()> {
        let outputs = File::create(&self.log_dir)?;
        let errors = outputs.try_clone()?;

        let launcher = self.launcher.clone();
        let mut binary = {
            let base = launcher.ok_or(LocalRuntimeError::MissingFluvioRunner)?;
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
        info!("SPU log generated at {}", self.log_dir);
        info!(cmd = %cmd.display(),"Invoking command");
        cmd.stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()
            .map_err(|_| LocalRuntimeError::Other("SPU server failed to start".to_string()))?;
        Ok(())
    }

    fn stop(&self) -> AnyResult<()> {
        Ok(())
    }

    fn id(&self) -> SpuId {
        self.id
    }

    fn spec(&self) -> &SpuSpec {
        &self.spec
    }
}

const BASE_SPU: u16 = 5001;
/// manage spu process cluster

pub struct LocalSpuProcessClusterManager {
    pub log_dir: PathBuf,
    pub launcher: Option<PathBuf>,
    pub rust_log: String,
    pub data_dir: PathBuf,
    pub tls_policy: TlsPolicy,
}

impl SpuClusterManager for LocalSpuProcessClusterManager {
    fn create_spu_relative(&self, relative_id: u16) -> Box<dyn SpuTarget> {
        self.create_spu_absolute(relative_id + BASE_SPU)
    }

    fn create_spu_absolute(&self, id: u16) -> Box<dyn SpuTarget> {
        let spu_index = id - BASE_SPU;
        let public_port = SPU_PUBLIC_PORT + spu_index * 10;
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

        let spu_log_dir = format!("{}/spu_log_{}.log", self.log_dir.display(), id);

        Box::new(LocalSpuProcess {
            id: spu_spec.id,
            spec: spu_spec,
            log_dir: spu_log_dir,
            rust_log: self.rust_log.clone(),
            launcher: self.launcher.clone(),
            tls_policy: self.tls_policy.clone(),
            data_dir: self.data_dir.clone(),
        })
    }

    fn terminate_spu(&self, id: SpuId) -> AnyResult<()> {
        let kill_arg = format!("fluvio-run spu -i {id}");
        Command::new("pkill")
            .arg("-f")
            .arg(kill_arg)
            .output()
            .map_err(|err| anyhow!("failed to terminate: {err}"))
            .map(|_| ())
    }
}
