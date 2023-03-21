use std::convert::TryInto;

use clap::Parser;
use tracing::debug;
use anyhow::{Result, anyhow};

use fluvio::config::{Profile, ConfigFile};
use fluvio::FluvioConfig;
use k8_config::K8Config;
use k8_client::K8Client;
use k8_client::meta_client::MetadataClient;
use k8_types::core::service::ServiceSpec;
use k8_types::InputObjectMeta;

use crate::common::tls::TlsClientOpt;

#[derive(Debug, Parser, Default)]
pub struct K8Opt {
    /// kubernetes namespace,
    #[clap(long, short, value_name = "namespace")]
    pub namespace: Option<String>,

    /// profile name
    #[clap(value_name = "name")]
    pub name: Option<String>,

    #[clap(flatten)]
    pub tls: TlsClientOpt,
}

impl K8Opt {
    pub async fn process(self) -> Result<()> {
        let external_addr = match discover_fluvio_addr(self.namespace.as_deref()).await? {
            Some(sc_addr) => sc_addr,
            None => return Err(anyhow!("fluvio service is not deployed")),
        };

        match set_k8_context(self, external_addr).await {
            Ok(profile) => {
                println!("updated profile: {profile:#?}");
            }
            Err(err) => {
                eprintln!("config creation failed: {err}");
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
        K8Config::Pod(_) => return Err(anyhow!("Pod config is not valid here")),
        K8Config::KubeConfig(config) => config,
    };

    if let Some(ctx) = kc_config.config.current_context() {
        Ok(ctx.name.to_owned())
    } else {
        Err(anyhow!("no context found"))
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
            cluster.endpoint = external_addr;
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
    let svc = match K8Client::try_default()?
        .retrieve_item::<ServiceSpec, _>(&InputObjectMeta::named("fluvio-sc-public", ns))
        .await
    {
        Ok(svc) => svc,
        Err(err) => match err {
            k8_client::ClientError::ApiResponse(status)
                if status.code == Some(StatusCode::NOT_FOUND.as_u16()) =>
            {
                return Ok(None)
            }
            _ => return Err(anyhow!("unable to look up fluvio service in k8: {}", err)),
        },
    };

    debug!("fluvio svc: {:#?}", svc);

    let ingress_addr = svc
        .status
        .load_balancer
        .ingress
        .get(0)
        .and_then(|ingress| ingress.host_or_ip());

    let target_port = svc
        .spec
        .ports
        .get(0)
        .and_then(|port| port.target_port.as_ref());

    let address = match (ingress_addr, target_port) {
        (Some(addr), Some(port)) => Some(format!("{addr}:{port}")),
        _ => None,
    };

    Ok(address)
}
