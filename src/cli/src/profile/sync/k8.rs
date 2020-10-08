use std::convert::TryInto;
use structopt::StructOpt;
use tracing::debug;

use fluvio::config::{Profile, ConfigFile};
use fluvio::FluvioConfig;
use k8_config::K8Config;
use k8_client::K8Client;
use k8_client::metadata::MetadataClient;
use fluvio_controlplane_metadata::k8::core::service::ServiceSpec;
use fluvio_controlplane_metadata::k8::metadata::InputObjectMeta;
use crate::{Result, CliError};
use crate::tls::TlsClientOpt;

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

impl K8Opt {
    pub async fn process(self) -> Result<()> {
        let external_addr = match discover_fluvio_addr(self.namespace.as_deref()).await? {
            Some(sc_addr) => sc_addr,
            None => {
                return Err(CliError::Other(
                    "fluvio service is not deployed".to_string(),
                ))
            }
        };

        match set_k8_context(self, external_addr).await {
            Ok(profile) => {
                println!("updated profile: {:#?}", profile);
            }
            Err(err) => {
                eprintln!("config creation failed: {}", err);
            }
        }
        Ok(())
    }
}

/// compute profile name, if name exists in the cli option, we use that
/// otherwise, we look up k8 config context name
fn compute_profile_name() -> Result<String> {
    let k8_config = K8Config::load()?;

    let kc_config = match k8_config {
        K8Config::Pod(_) => return Err(CliError::Other("Pod config is not valid here".to_owned())),
        K8Config::KubeConfig(config) => config,
    };

    if let Some(ctx) = kc_config.config.current_context() {
        Ok(ctx.name.to_owned())
    } else {
        Err(CliError::Other("no context found".to_owned()))
    }
}

/// create new k8 cluster and profile
pub async fn set_k8_context(opt: K8Opt, external_addr: String) -> Result<Profile> {
    let mut config_file = ConfigFile::load_default_or_new()?;
    let config = config_file.mut_config();

    let profile_name = if let Some(name) = &opt.name {
        name.to_owned()
    } else {
        compute_profile_name()?
    };

    match config.cluster_mut(&profile_name) {
        Some(cluster) => {
            cluster.addr = external_addr;
            cluster.tls = opt.tls.try_into()?;
        }
        None => {
            let mut local_cluster = FluvioConfig::new(external_addr);
            local_cluster.tls = opt.tls.try_into()?;
            config.add_cluster(local_cluster, profile_name.clone());
        }
    };

    // check if we local profile exits otherwise, create new one, then set name as cluster
    let new_profile = match config.profile_mut(&profile_name) {
        Some(profile) => {
            profile.set_cluster(profile_name.clone());
            profile.clone()
        }
        None => {
            let profile = Profile::new(profile_name.clone());
            config.add_profile(profile.clone(), profile_name.clone());
            profile
        }
    };

    // finally we set current profile to local
    assert!(config.set_current_profile(&profile_name));

    config_file.save()?;

    println!("k8 profile set");

    Ok(new_profile)
}

/// find fluvio addr
pub async fn discover_fluvio_addr(namespace: Option<&str>) -> Result<Option<String>> {
    use k8_client::http::status::StatusCode;

    let ns = namespace.unwrap_or("default");
    let svc = match K8Client::default()?
        .retrieve_item::<ServiceSpec, _>(&InputObjectMeta::named("flv-sc-public", ns))
        .await
    {
        Ok(svc) => svc,
        Err(err) => match err {
            k8_client::ClientError::Client(status) if status == StatusCode::NOT_FOUND => {
                return Ok(None)
            }
            _ => {
                return Err(CliError::Other(format!(
                    "unable to look up fluvio service in k8: {}",
                    err
                )))
            }
        },
    };

    debug!("fluvio svc: {:#?}", svc);

    let ingress_addr = match svc.status.load_balancer.ingress.iter().find(|_| true) {
        Some(ingress) => ingress.host_or_ip().map(|addr| addr.to_owned()),
        None => None,
    };

    Ok(if let Some(external_address) = ingress_addr {
        // find target port
        if let Some(port) = svc.spec.ports.iter().find(|_| true) {
            if let Some(target_port) = port.target_port {
                Some(format!("{}:{}", external_address, target_port))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    })
}
