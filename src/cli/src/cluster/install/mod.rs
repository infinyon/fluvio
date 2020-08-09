mod k8;

#[cfg(feature = "cluster_feature")]
mod local;

mod helm;

use structopt::StructOpt;

use crate::Terminal;
use crate::CliError;

use super::util::*;

#[derive(Debug, StructOpt)]
pub struct K8Install {
    /// k8: use specific release version
    #[structopt(long)]
    pub version: Option<String>,

    /// k8
    #[structopt(long, default_value = "default")]
    pub namespace: String,

    /// k8
    #[structopt(long, default_value = "main")]
    pub group_name: String,

    /// helm chart name
    #[structopt(long, default_value = "fluvio")]
    pub chart_name: String,

    #[structopt(long)]
    pub chart_location: Option<String>,

    /// k8
    #[structopt(long, default_value = "minikube")]
    pub cloud: String,
}

#[derive(Debug, StructOpt)]
pub struct TlsConfig {
    /// tls
    #[structopt(long)]
    tls: bool,

    /// TLS: path to server certificate
    #[structopt(long, required_if("tls", "true"))]
    pub server_cert: Option<String>,

    /// TLS: path to server private key
    #[structopt(long, required_if("tls", "true"))]
    pub server_key: Option<String>,

    /// TLS: domain
    #[structopt(long, required_if("tls", "true"))]
    pub domain: Option<String>,

    /// TLS: client cert
    #[structopt(long, required_if("tls", "true"))]
    pub client_cert: Option<String>,

    /// TLS: client key
    #[structopt(long, required_if("tls", "true"))]
    pub client_key: Option<String>,

    /// TLS: ca cert
    #[structopt(long, required_if("tls", "true"))]
    pub ca_cert: Option<String>,
}

#[derive(Debug, StructOpt)]
pub struct InstallCommand {
    /// use local image
    #[structopt(long)]
    pub develop: bool,

    #[structopt(flatten)]
    pub k8_config: K8Install,

    /// number of SPU
    #[structopt(long, default_value = "1")]
    spu: u16,

    /// RUST_LOG
    #[structopt(long)]
    log: Option<String>,

    #[structopt(long)]
    /// installing sys
    sys: bool,

    /// install local spu/sc(custom)
    #[structopt(long)]
    local: bool,

    #[structopt(flatten)]
    tls: TlsConfig,
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
    

    #[cfg(feature = "cluster_feature")]
    use local::install_local;

    if command.sys {
        install_sys(command);
    } else {
        if command.local {

            #[cfg(feature = "cluster_feature")]
            install_local(command).await?;
        } else {
            install_core(command).await?;
        }
    }

    Ok("".to_owned())
}
