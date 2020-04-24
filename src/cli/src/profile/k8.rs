use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;


use k8_client::K8Client;


use k8_obj_core::service::ServiceSpec;
use k8_obj_metadata::InputObjectMeta;
use k8_client::metadata::MetadataClient;
use k8_client::ClientError as K8ClientError;


/// find fluvio addr
pub async fn discover_fluvio_addr(client: &K8Client,namespace: Option<String>) -> Result<Option<String>,IoError> {

    let ns = namespace.unwrap_or("default".to_owned());
    let svc = match client.retrieve_item::<ServiceSpec,_>(&InputObjectMeta::named("flv-sc-public", &ns)).await {
        Ok(svc) => svc,
        Err(err) => match err {
            K8ClientError::NotFound => return Ok(None),
            _ => return Err(IoError::new(ErrorKind::Other,format!("unable to look up fluvio service in k8: {}",err.to_string())))
        }
    };

    debug!("fluvio svc: {:#?}",svc);

    let ingress_addr = match svc.status.load_balancer.ingress.iter().find(|_| true) {
        Some(ingress) =>  ingress.host_or_ip().map(|addr| addr.to_owned()),
        None => None
    };

    Ok(
        if let Some(external_address) = ingress_addr {
            // find target port
            if let Some(port) = svc.spec.ports.iter().find(|_| true) {
                if let Some(target_port) = port.target_port {
                    Some(format!("{}:{}",external_address,target_port))
                } else {
                    None
                }
            } else {
                None
            }
            
        } else {
            None
        }
    )
}