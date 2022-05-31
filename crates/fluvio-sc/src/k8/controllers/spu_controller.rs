use std::{collections::HashMap, fmt, net::IpAddr, time::Duration};

use fluvio_controlplane_metadata::{
    spu::{IngressPort},
    store::{MetadataStoreObject, k8::K8MetaItem},
};

use fluvio_stream_dispatcher::actions::WSAction;
use k8_client::ClientError;
use tracing::{debug, error, instrument, info};

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use k8_types::core::service::{LoadBalancerIngress, LoadBalancerType};

use crate::{
    stores::{StoreContext},
};
use crate::stores::spu::{IngressAddr, SpuSpec};
use crate::k8::objects::spu_service::SpuServiceSpec;
use crate::k8::objects::spg_group::SpuGroupObj;
use crate::stores::spg::{SpuGroupSpec};

/// Update SPU from changes in SPU Group and SPU Services
/// This is only place where we make changes to SPU
/// For each SPU Group, we map to children SPUs so it's 1:M parent-child relationship
pub struct K8SpuController {
    services: StoreContext<SpuServiceSpec>,
    groups: StoreContext<SpuGroupSpec>,
    spus: StoreContext<SpuSpec>,
}

impl fmt::Display for K8SpuController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpgSpuController")
    }
}

impl fmt::Debug for K8SpuController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpgSpuController")
    }
}

impl K8SpuController {
    pub fn start(
        spus: StoreContext<SpuSpec>,
        services: StoreContext<SpuServiceSpec>,
        groups: StoreContext<SpuGroupSpec>,
    ) {
        let controller = Self {
            services,
            groups,
            spus,
        };

        spawn(controller.dispatch_loop());
    }

    #[instrument(skip(self), name = "SpgSpuController")]
    async fn dispatch_loop(mut self) {
        info!("started");
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with inner loop: {:#?}", err);
                debug!("sleeping 10 seconds try again");
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    async fn inner_loop(&mut self) -> Result<(), ClientError> {
        use tokio::select;

        debug!("initializing listeners");
        let mut service_listener = self.services.change_listener();
        let _ = service_listener.wait_for_initial_sync().await;

        let mut spg_listener = self.groups.change_listener();
        let _ = spg_listener.wait_for_initial_sync().await;

        let mut spu_listener = self.spus.change_listener();
        let _ = spu_listener.wait_for_initial_sync().await;

        debug!("finish initializing listeners");

        loop {
            self.sync_spu().await?;

            debug!("waiting events");

            select! {


                _ = service_listener.listen() => {
                    debug!("detected spu service changes");

                    let changes = service_listener.sync_changes().await;
                    let epoch = changes.epoch;
                    let (updates, deletes) = changes.parts();

                    debug!(
                        "received spu service changes updates: {}, deletes: {}, epoch: {}",
                        updates.len(),
                        deletes.len(),
                        epoch,
                    );

                },

                _ = spg_listener.listen() => {
                    debug!("detected spg changes");

                    let changes = spg_listener.sync_changes().await;
                    let epoch = changes.epoch;
                    let (updates, deletes) = changes.parts();

                    debug!(
                        "received spg group changes updates: {},deletes: {},epoch: {}",
                        updates.len(),
                        deletes.len(),
                        epoch,
                    );
                },


                _ = spu_listener.listen() => {
                    debug!("detected spu changes");

                    // only care about spec changes
                    let changes = spu_listener.sync_spec_changes().await;
                    let epoch = changes.epoch;
                    let (updates, deletes) = changes.parts();

                    debug!(
                        "received spu changes updates: {}, deletes: {}, epoch: {}",
                        updates.len(),
                        deletes.len(),
                        epoch,
                    );


                    debug!("spu was already initialized, ignoring");

                }



            }
        }
    }

    #[instrument(skip(self))]
    /// synchronize change from spg to spu
    async fn sync_spu(&mut self) -> Result<(), ClientError> {
        // get all models
        let spg = self.groups.store().clone_values().await;
        let services = self.get_spu_services().await;

        debug!(spg = spg.len(), services = services.len(), "starting sync");

        for group_item in spg.into_iter() {
            let spg_obj = SpuGroupObj::new(group_item);

            let spec = spg_obj.spec();
            let replicas = spec.replicas;
            for i in 0..replicas {
                let (spu_name, spu) = spg_obj.as_spu(i, &services);

                debug!(id=i,spu=?spu,"applying spu");

                self.spus
                    .wait_action(&spu_name, WSAction::Apply(spu))
                    .await?;
            }
        }

        Ok(())
    }

    /// map spu services to hashmap
    async fn get_spu_services(&self) -> HashMap<String, IngressPort> {
        let services = self.services.store().clone_values().await;
        let mut spu_services = HashMap::new();

        for svc_md in services.into_iter() {
            if let Some(spu_name) = SpuServiceSpec::spu_name(svc_md.ctx().item().inner()) {
                match get_ingress_from_service(&svc_md) {
                    Ok(ingress) => {
                        spu_services.insert(spu_name.to_owned(), ingress);
                    }
                    Err(err) => {
                        error!("error reading ingress: {}", err);
                    }
                }
            } else {
                error!(
                    svc = %svc_md.key(),
                    "spu service doesnt have spu name",
                );
            }
        }

        spu_services
    }
}

fn get_ingress_from_service(
    svc_md: &MetadataStoreObject<SpuServiceSpec, K8MetaItem>,
) -> Result<IngressPort, ClientError> {
    // Get the external ingress from the service
    // Look at svc_md to identify if LoadBalancer
    let lb_type = svc_md.spec().inner().r#type.as_ref();

    // This will either have a value from External-IP, or will be empty
    // If empty, the use `fluvio.io/ingress-address` annotation to set an external address for SPU
    let svc_lb_ingresses = svc_md.status.ingress();

    // Choose the external port based on the service type
    let mut computed_spu_ingressport = match lb_type {
        Some(LoadBalancerType::NodePort) => {
            let port = svc_md.spec().inner().ports[0]
                .node_port
                .ok_or_else(|| ClientError::Other("SPU service missing NodePort".into()))?;
            IngressPort {
                port,
                ..Default::default()
            }
        }
        _ => {
            let port = svc_md.spec().inner().ports[0].port;
            IngressPort {
                port,
                ingress: svc_lb_ingresses.iter().map(convert).collect(),
                ..Default::default()
            }
        }
    };

    // Add additional ingress via annotation value
    add_ingress_from_svc_annotation(svc_md, &mut computed_spu_ingressport.ingress);

    debug!(
        "Computed SPU ingress after applying any svc annotation: {:?}",
        &computed_spu_ingressport
    );

    Ok(computed_spu_ingressport)
}

fn add_ingress_from_svc_annotation(
    svc_md: &MetadataStoreObject<SpuServiceSpec, K8MetaItem>,
    computed_spu_ingress: &mut Vec<IngressAddr>,
) {
    if let Some(address) = SpuServiceSpec::ingress_annotation(svc_md.ctx().item()) {
        if let Ok(ip_addr) = address.parse::<IpAddr>() {
            computed_spu_ingress.push(IngressAddr {
                hostname: None,
                ip: Some(ip_addr.to_string()),
            });
        } else {
            computed_spu_ingress.push(IngressAddr {
                hostname: Some(address.clone()),
                ip: None,
            });
        }
    }
}

fn convert(ingress_addr: &LoadBalancerIngress) -> IngressAddr {
    IngressAddr {
        hostname: ingress_addr.hostname.clone(),
        ip: ingress_addr.ip.clone(),
    }
}
