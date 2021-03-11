use std::{fmt, time::Duration};

use fluvio_controlplane_metadata::{
    spg::SpuEndpointTemplate,
    spu::{Endpoint, IngressPort, SpuType},
    store::MetadataStoreObject,
};
use fluvio_stream_dispatcher::actions::WSAction;
use fluvio_types::SpuId;
use k8_client::{ClientError};
use tracing::debug;
use tracing::trace;
use tracing::error;
use tracing::instrument;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use k8_types::core::service::LoadBalancerIngress;

use crate::{
    stores::{StoreContext, K8ChangeListener},
};
use crate::stores::spu::{IngressAddr, SpuSpec};
use crate::k8::objects::spu_service::SpuServicespec;
use crate::k8::objects::spg_group::SpuGroupObj;
use crate::stores::spg::{SpuGroupSpec};

/// Maintain Managed SPU
/// sync from spu services and statefulset
pub struct SpuController {
    services: StoreContext<SpuServicespec>,
    groups: StoreContext<SpuGroupSpec>,
    spus: StoreContext<SpuSpec>,
}

impl fmt::Display for SpuController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpuController")
    }
}

impl fmt::Debug for SpuController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpuController")
    }
}

impl SpuController {
    pub fn start(
        spus: StoreContext<SpuSpec>,
        services: StoreContext<SpuServicespec>,
        groups: StoreContext<SpuGroupSpec>,
    ) {
        let controller = Self {
            services,
            spus,
            groups,
        };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with inner loop: {:#?}", err);
                debug!("sleeping 1 miniute to try again");
                sleep(Duration::from_secs(60)).await;
            }
        }
    }

    #[instrument(skip(self), name = "SpuSvcLoop")]
    async fn inner_loop(&mut self) -> Result<(), ClientError> {
        use tokio::select;

        let mut service_listener = self.services.change_listener();
        let mut spg_listener = self.groups.change_listener();

        self.sync_from_spu_services(&mut service_listener).await?;
        self.sync_with_spg(&mut spg_listener).await?;

        loop {
            trace!("waiting events");

            select! {

                _ = service_listener.listen() => {
                    debug!("detected spu service changes");
                    self.sync_from_spu_services(&mut service_listener).await?;
                },

                _ = spg_listener.listen() => {
                    debug!("detected spg changes");
                    self.sync_with_spg(&mut spg_listener).await?;
                }

            }
        }
    }

    /// svc has been changed, update spu
    async fn sync_from_spu_services(
        &mut self,
        listener: &mut K8ChangeListener<SpuServicespec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no service change, skipping");
            return Ok(());
        }

        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received spu service changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        for svc_md in updates.into_iter() {
            // check if
            let spu_id = svc_md.key();
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

        Ok(())
    }

    async fn sync_with_spg(
        &mut self,
        listener: &mut K8ChangeListener<SpuGroupSpec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no service change, skipping");
            return Ok(());
        }

        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received statefulset changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        for group_item in updates.into_iter() {
            let spg_obj = SpuGroupObj::new(group_item);

            let spec = spg_obj.spec();
            let replicas = spec.replicas;
            for i in 0..replicas {
                let spu_id = compute_spu_id(spec.min_id, i);
                let spu_name = format!("{}-{}", spg_obj.key(), i);
                debug!(%spu_name,"generating spu with name");

                self.apply_spu(&spg_obj, &spu_name, spu_id).await?;
            }
        }

        Ok(())
    }

    async fn apply_spu(
        &self,
        spg_obj: &SpuGroupObj,
        spu_name: &String,
        id: SpuId,
    ) -> Result<(), ClientError> {
        let spu_private_ep = SpuEndpointTemplate::default_private();
        let spu_public_ep = SpuEndpointTemplate::default_public();

        let full_group_name = format!("fluvio-spg-{}", spg_obj.key());
        let full_spu_name = format!("fluvio-spg-{}", spu_name);
        let spu_spec = SpuSpec {
            id,
            spu_type: SpuType::Managed,
            public_endpoint: IngressPort {
                port: spu_public_ep.port,
                encryption: spu_public_ep.encryption,
                ingress: vec![],
            },
            private_endpoint: Endpoint {
                host: format!("{}.{}", full_spu_name, full_group_name),
                port: spu_private_ep.port,
                encryption: spu_private_ep.encryption,
            },
            rack: None,
        };

        let action = WSAction::Apply(
            MetadataStoreObject::with_spec(spu_name, spu_spec)
                .with_context(spg_obj.ctx().create_child()),
        );

        trace!("spu action: {:#?}", action);

        self.spus.wait_action(spu_name, action).await?;

        Ok(())
    }
}

fn convert(ingress_addr: &LoadBalancerIngress) -> IngressAddr {
    IngressAddr {
        hostname: ingress_addr.hostname.clone(),
        ip: ingress_addr.ip.clone(),
    }
}

/// compute spu id with min_id as base
fn compute_spu_id(min_id: i32, replica_index: u16) -> i32 {
    replica_index as i32 + min_id
}
