use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;

use k8_client::K8Client;

use k8_obj_core::service::ServiceSpec;
use k8_obj_metadata::InputObjectMeta;
use k8_client::metadata::MetadataClient;
use k8_client::ClientError as K8ClientError;
use k8_client::K8Config;

use flv_client::config::*;


use super::cli::SetK8;

/// compute profile name, if name exists in the cli option, we use that
/// otherwise, we look up k8 config context name
fn compute_profile_name(opt: &super::cli::SetK8, k8_config: &K8Config) -> Result<String, IoError> {
    if let Some(name) = &opt.name {
        return Ok(name.to_owned());
    }

    let kc_config = match k8_config {
        K8Config::Pod(_) => {
            return Err(IoError::new(
                ErrorKind::Other,
                "Pod config is not valid here",
            ))
        }
        K8Config::KubeConfig(config) => config,
    };

    if let Some(ctx) = kc_config.config.current_context() {
        Ok(ctx.name.to_owned())
    } else {
        Err(IoError::new(ErrorKind::Other, "no context found"))
    }
}

/// create new k8 cluster and profile
pub async fn set_k8_context(opt: SetK8) -> Result<String, IoError> {
    let mut config_file = ConfigFile::load_default_or_new()?;

    let k8_config = K8Config::load().map_err(|err| {
        IoError::new(
            ErrorKind::Other,
            format!("unable to load kube context {}", err),
        )
    })?;

    let profile_name = compute_profile_name(&opt, &k8_config)?;

    let k8_client = K8Client::new(k8_config).map_err(|err| {
        IoError::new(
            ErrorKind::Other,
            format!("unable to create kubernetes client: {}", err),
        )
    })?;

    let external_addr =
        if let Some(sc_addr) = discover_fluvio_addr(&k8_client, opt.namespace).await? {
            sc_addr
        } else {
            return Err(IoError::new(
                ErrorKind::Other,
                format!("fluvio service is not deployed"),
            ));
        };

    debug!("found sc_addr is: {}", external_addr);

    let config = config_file.mut_config();

    match config.mut_cluster(&profile_name) {
        Some(cluster) => {
            cluster.set_addr(external_addr.clone());
            cluster.tls = opt.tls.try_into_inline()?;
        }
        None => {
            let mut local_cluster = Cluster::new(external_addr.clone());
            local_cluster.tls = opt.tls.try_into_inline()?;
            config.add_cluster(local_cluster, profile_name.clone());
        }
    };

    // check if we local profile exits otherwise, create new one, then set name as cluster
    match config.mut_profile(&profile_name) {
        Some(profile) => {
            profile.set_cluster(profile_name.clone());
        }
        None => {
            let profile = Profile::new(profile_name.clone());
            config.add_profile(profile, profile_name.clone());
        }
    }

    // finally we set current profile to local
    assert!(config.set_current_profile(&profile_name));

    config_file.save()?;

    Ok(format!(
        "new cluster/profile: {} is set to: {}",
        profile_name, external_addr
    ))
}

/// find fluvio addr
pub async fn discover_fluvio_addr(
    client: &K8Client,
    namespace: Option<String>,
) -> Result<Option<String>, IoError> {
    let ns = namespace.unwrap_or("default".to_owned());
    let svc = match client
        .retrieve_item::<ServiceSpec, _>(&InputObjectMeta::named("flv-sc-public", &ns))
        .await
    {
        Ok(svc) => svc,
        Err(err) => match err {
            K8ClientError::NotFound => return Ok(None),
            _ => {
                return Err(IoError::new(
                    ErrorKind::Other,
                    format!(
                        "unable to look up fluvio service in k8: {}",
                        err.to_string()
                    ),
                ))
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
