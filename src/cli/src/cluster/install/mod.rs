#[cfg(feature = "cluster_components")]
mod local;
mod k8;
mod tls;

use structopt::StructOpt;

use crate::Terminal;
use crate::CliError;
pub use tls::TlsOpt;

use super::util::*;

#[derive(Debug, StructOpt)]
pub struct K8Install {
    /// k8: use specific chart version
    #[structopt(long)]
    pub chart_version: Option<String>,

    /// k8: use specific image version
    #[structopt(long)]
    pub image_version: Option<String>,

    /// k8: use custom docker registry
    #[structopt(long)]
    pub registry: Option<String>,

    /// k8
    #[structopt(long, default_value = "default")]
    pub namespace: String,

    /// k8
    #[structopt(long, default_value = "main")]
    pub group_name: String,

    /// helm chart installation name
    #[structopt(long, default_value = "fluvio")]
    pub install_name: String,

    /// Local path to a helm chart to install
    #[structopt(long)]
    pub chart_location: Option<String>,

    /// k8
    #[structopt(long, default_value = "minikube")]
    pub cloud: String,
}

#[derive(Debug, StructOpt)]
pub struct InstallCommand {
    /// use local image
    #[structopt(long)]
    pub develop: bool,

    #[structopt(flatten)]
    pub k8_config: K8Install,

    #[structopt(long)]
    pub skip_profile_creation: bool,

    /// number of SPU
    #[structopt(long, default_value = "1")]
    pub spu: u16,

    /// RUST_LOG options
    #[structopt(long)]
    pub rust_log: Option<String>,

    /// log dir
    #[structopt(long)]
    log_dir: Option<String>,

    #[structopt(long)]
    /// installing sys
    sys: bool,

    /// install local spu/sc(custom)
    #[structopt(long)]
    local: bool,

    #[structopt(flatten)]
    tls: TlsOpt,
}

pub async fn process_install<O>(
    _out: std::sync::Arc<O>,
    command: InstallCommand,
) -> Result<String, CliError>
where
    O: Terminal,
{
    use k8::install_sys;
    use k8::install_core;

    let spu = command.spu;

    #[cfg(feature = "cluster_components")]
    use local::install_local;

    if command.sys {
        install_sys(command)?;
    } else if command.local {
        #[cfg(feature = "cluster_components")]
        install_local(command).await?;
        confirm_spu(spu).await?;
    } else {
        install_core(command).await?;
        confirm_spu(spu).await?;
    }

    Ok("".to_owned())
}

/// check to ensure spu are all running
async fn confirm_spu(spu: u16) -> Result<(), CliError> {
    use std::time::Duration;

    use fluvio_future::timer::sleep;
    use fluvio::Fluvio;
    use fluvio_cluster::ClusterError;
    use fluvio_controlplane_metadata::spu::SpuSpec;

    println!("waiting for spu to be provisioned");

    let mut client = Fluvio::connect().await.expect("sc ");

    let mut admin = client.admin().await;

    // wait for list of spu
    for _ in 0..30u16 {
        let spus = admin.list::<SpuSpec, _>(vec![]).await.expect("no spu list");
        let live_spus = spus
            .iter()
            .filter(|spu| spu.status.is_online() && !spu.spec.public_endpoint.ingress.is_empty())
            .count();
        if live_spus == spu as usize {
            println!("{} spus provisioned", spus.len());
            return Ok(());
        } else {
            println!("{} out of spu: {} up, waiting 1 sec", live_spus, spu);
            sleep(Duration::from_secs(1)).await;
        }
    }

    println!("waited too long,bailing out");
    Err(CliError::ClusterError(ClusterError::Other(format!(
        "not able to provision:{} spu",
        spu
    ))))
}
