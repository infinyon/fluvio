mod k8;

#[cfg(feature = "cluster_components")]
mod local;

mod helm;

use structopt::StructOpt;

use crate::Terminal;
use crate::CliError;

use super::util::*;
use std::path::PathBuf;

pub use helm::installed_sys_charts;

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
pub struct TlsOpt {
    /// tls
    #[structopt(long)]
    tls: bool,

    /// TLS: path to server certificate
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub server_cert: Option<PathBuf>,

    /// TLS: path to server private key
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub server_key: Option<PathBuf>,

    /// TLS: domain
    #[structopt(long, required_if("tls", "true"))]
    pub domain: Option<String>,

    /// TLS: client cert
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub client_cert: Option<PathBuf>,

    /// TLS: client key
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub client_key: Option<PathBuf>,

    /// TLS: ca cert
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub ca_cert: Option<PathBuf>,
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

    #[cfg(feature = "cluster_components")]
    use local::install_local;

    if command.sys {
        install_sys(command)?;
    } else if command.local {
        #[cfg(feature = "cluster_components")]
        install_local(command).await?;
    } else {
        install_core(command).await?;
    }

    Ok("".to_owned())
}
