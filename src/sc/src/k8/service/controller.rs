use std::fmt;

use tracing::debug;
use tracing::trace;
use tracing::error;
use tracing::instrument;

use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::{ StoreContext, StoreChanges};
use crate::stores::event::ChangeListener;
use crate::stores::spu::IngressAddr;
use crate::stores::spu::SpuSpec;
use crate::dispatcher::k8::core::service::LoadBalancerIngress;

use super::SpuServicespec;

/// Controleller to sync Spu and Svc
/// It is used to update SPU's public ip address from external load balancer service.
/// External load balancer update external ip or hostname out of band.
pub struct SpuServiceController {
    services: StoreContext<SpuServicespec>,
    spus: StoreContext<SpuSpec>,
}

impl fmt::Display for SpuServiceController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ServiceController")
    }
}

impl fmt::Debug for SpuServiceController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ServiceController")
    }
}

impl SpuServiceController {
    pub fn start(ctx: SharedContext, services: StoreContext<SpuServicespec>) {
        let spus = ctx.spus().clone();

        let controller = Self { services, spus };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        use std::time::Duration;

        use tokio::select;
        use fluvio_future::timer::sleep;

        let mut service_spec_listener = self.services.spec_listen();
        let mut service_status_listener = self.services.status_listen();
        let mut spu_spec_listener = self.spus.spec_listen();
        let mut spu_status_listener = self.spus.status_listen();

        loop {
            self.sync_service_to_spu(&mut service_spec_listener, &mut service_status_listener)
                .await;
            self.sync_spu_to_service(&mut spu_spec_listener, &mut spu_status_listener)
                .await;

            debug!("waiting events");

            select! {
                // just in case, we force
                _ = sleep(Duration::from_secs(60)) => {
                    debug!("timer expired");
                },
                _ = service_spec_listener.listen() => {
                    debug!("detected service spec changes");
                },
                _ = service_status_listener.listen() => {
                    debug!("detected service status changes");
                },
                _ = spu_spec_listener.listen() => {
                    debug!("detected spu spec changes");
                },
                _ = spu_status_listener.listen() => {
                    debug!("detected spu status changes");
                }
            }
        }
    }

    /// svc has been changed, update spu
    async fn sync_service_to_spu(
        &mut self,
        spec: &mut ChangeListener,
        status: &mut ChangeListener,
    ) {

        if spec.has_change() {
            self.sync_service_to_spu_changes(self.services.store().spec_changes_since(spec).await).await;
        }

        if status.has_change() {
            self.sync_service_to_spu_changes(self.services.store().status_changes_since(status).await).await;
        }
        
    }

    #[instrument(skip(self))]
    async fn sync_service_to_spu_changes(
        &mut self,
        changes: StoreChanges<SpuServicespec>
    ) {
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received service changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        for svc_md in updates.into_iter() {
            // check if
            let spu_id = &svc_md.spec.spu_name;
            // check if ingress exists
            let svc_ingresses = svc_md.status.ingress();

            if let Some(mut spu) = self.spus.store().value(spu_id).await {
                debug!(
                    "trying sync service: {}, with: spu: {}",
                    svc_md.key(),
                    spu_id
                );
                trace!("svc ingress: {:#?}", svc_ingresses);
                let spu_ingress = svc_ingresses.iter().map(convert).collect();
                trace!("spu ingress: {:#?}", spu_ingress);
                if spu_ingress != spu.spec.public_endpoint.ingress {
                    debug!(
                        "updating spu:{} public end point: {:#?}",
                        spu_id, spu_ingress
                    );
                    spu.spec.public_endpoint.ingress = spu_ingress;
                    if let Err(err) = self
                        .spus
                        .create_spec(spu_id.to_owned(), spu.spec.clone())
                        .await
                    {
                        error!("error applying spec: {}", err);
                    }
                } else {
                    debug!("detected no spu: {} ingress changes", spu_id);
                }
            } else {
                debug!(
                    "no sync service: {}, with: spu: {} because spu doesn't exist",
                    svc_md.key(),
                    spu_id
                );
            }
        }
    }

    #[instrument()]
    /// spu has been changed, sync with existing services
    async fn sync_spu_to_service(
        &mut self,
        spec: &mut ChangeListener,
        status: &mut ChangeListener,
    ) {
        if spec.has_change() {
            self.sync_spu_to_service_changes(self.spus.store().spec_changes_since(spec).await).await;
        }

        if status.has_change() {
            self.sync_spu_to_service_changes(self.spus.store().status_changes_since(status).await).await;
        }
    }


    async fn sync_spu_to_service_changes(
        &mut self,
        changes: StoreChanges<SpuSpec>
    ) {
        debug!("synching spu to service");

        let epoch = changes.epoch; 
        let (updates, deletes) = changes.parts();
        debug!(
            "received spu changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch
        );

        for spu_md in updates.into_iter() {
            let spu_id = spu_md.key();
            // check if ingress exists
            let spu_ingress = &spu_md.spec.public_endpoint.ingress;

            if let Some(svc) = self.services.store().value(spu_id).await {
                // apply ingress
                let svc_ingresses = svc.status.ingress();
                let computed_spu_ingress: Vec<IngressAddr> =
                    svc_ingresses.iter().map(convert).collect();
                if &computed_spu_ingress != spu_ingress {
                    let mut update_spu = spu_md.spec.clone();
                    debug!(
                        "updating spu:{} public end point: {:#?} from svc: {}",
                        spu_id,
                        computed_spu_ingress,
                        svc.key()
                    );
                    update_spu.public_endpoint.ingress = computed_spu_ingress;
                    if let Err(err) = self.spus.create_spec(spu_id.to_owned(), update_spu).await {
                        error!("error applying spec: {}", err);
                    }
                } else {
                    debug!(
                        "detected no spu: {} ingress changes with svc: {}",
                        spu_id,
                        svc.key()
                    );
                }
            } else {
                debug!("no svc exists for spu {},skipping", spu_id);
            }
        }
    }
}

fn convert(ingress_addr: &LoadBalancerIngress) -> IngressAddr {
    IngressAddr {
        hostname: ingress_addr.hostname.clone(),
        ip: ingress_addr.ip.clone(),
    }
}
