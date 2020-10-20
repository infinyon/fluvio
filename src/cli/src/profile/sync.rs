use structopt::StructOpt;
use crate::{CliError, Terminal};
use crate::t_println;
use crate::COMMAND_TEMPLATE;
use crate::profile::{set_k8_context, discover_fluvio_addr, set_local_context};
use crate::tls::TlsClientOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "sync",
    template = COMMAND_TEMPLATE,
)]
pub enum SyncCommand {
    #[structopt(name = "k8", about = "sync profile from kubernetes cluster")]
    K8(K8Opt),
    #[structopt(name = "local", about = "sync profile from local cluster")]
    Local(LocalOpt),
}

#[derive(Debug, StructOpt, Default)]
pub struct K8Opt {
    /// kubernetes namespace,
    #[structopt(long, short, value_name = "namespace")]
    pub namespace: Option<String>,

    /// profile name
    #[structopt(value_name = "name")]
    pub name: Option<String>,

    #[structopt(flatten)]
    pub tls: TlsClientOpt,
}

#[derive(Debug, Default, StructOpt)]
pub struct LocalOpt {
    #[structopt(value_name = "host:port", default_value = "localhost:9003")]
    pub local: String,

    #[structopt(flatten)]
    pub tls: TlsClientOpt,
}

pub async fn process_sync<O: Terminal>(
    out: std::sync::Arc<O>,
    command: SyncCommand,
) -> Result<String, CliError> {
    match command {
        SyncCommand::K8(opt) => process_k8(out, opt).await,
        SyncCommand::Local(opt) => process_local(out, opt).await,
    }
}

pub async fn process_k8<O: Terminal>(
    out: std::sync::Arc<O>,
    opt: K8Opt,
) -> Result<String, CliError> {
    let external_addr = match discover_fluvio_addr(opt.namespace.as_deref()).await? {
        Some(sc_addr) => sc_addr,
        None => {
            return Err(CliError::Other(
                "fluvio service is not deployed".to_string(),
            ))
        }
    };

    match set_k8_context(opt, external_addr).await {
        Ok(profile) => t_println!(out, "updated profile: {:#?}", profile),
        Err(err) => {
            eprintln!("config creation failed: {}", err);
        }
    }
    Ok("".to_owned())
}

pub async fn process_local<O: Terminal>(
    out: std::sync::Arc<O>,
    opt: LocalOpt,
) -> Result<String, CliError> {
    match set_local_context(opt) {
        Ok(msg) => t_println!(out, "{}", msg),
        Err(err) => {
            eprintln!("config creation failed: {}", err);
        }
    }
    Ok("".to_owned())
}
