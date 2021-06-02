use std::{fmt, net::IpAddr, time::Duration};

use fluvio_controlplane_metadata::{
    spg::SpuEndpointTemplate,
    spu::{Endpoint, IngressPort, SpuType},
    store::{MetadataStoreObject, k8::K8MetaItem},
};
use fluvio_stream_dispatcher::actions::WSAction;
use fluvio_types::SpuId;
use k8_client::ClientError;
use tracing::debug;
use tracing::trace;
use tracing::error;
use tracing::instrument;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use k8_types::core::service::{LoadBalancerIngress, LoadBalancerType};

use crate::{
    stores::{StoreContext, K8ChangeListener},
};
use crate::stores::spu::{IngressAddr, SpuSpec};
use crate::k8::objects::spu_service::SpuServiceSpec;
use crate::k8::objects::spg_group::SpuGroupObj;
use crate::stores::spg::{SpuGroupSpec};

/// Maintain Managed SPU
/// sync from spu services and statefulset
pub struct SpuController {
    services: StoreContext<SpuServiceSpec>,
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

    async fn dispatch_loop(mut self) {
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with inner loop: {:#?}", err);
                debug!("sleeping 1 miniute to try again");
                sleep(Duration::from_secs(60)).await;
            }
        }
    }

    #[instrument(skip(self), name = "SpuSpecLoop")]
    async fn inner_loop(&mut self) -> Result<(), ClientError> {
        use tokio::select;

        let mut service_listener = self.services.change_listener();
        let mut spg_listener = self.groups.change_listener();
        let mut spu_listener = self.spus.change_listener();

        self.sync_with_spg(&mut spg_listener).await?;
        self.sync_from_spu_services(&mut service_listener).await?;
        self.sync_spus(&mut spu_listener).await?;

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
                },


                _ = spu_listener.listen() => {
                    debug!("detected spu changes");
                    self.sync_spus(&mut spu_listener).await?;
                }


            }
        }
    }

    /// spu has been changed, update service
    async fn sync_spus(
        &mut self,
        listener: &mut K8ChangeListener<SpuSpec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no spu change, skipping");
            return Ok(());
        }

        // only care about spec changes
        let changes = listener.sync_spec_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received spu changes updates: {}, deletes: {}, epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        for spu_md in updates.into_iter() {
            let spu_id = spu_md.key();
            let spu_meta = spu_md.ctx().item().inner();
            let svc_name = SpuServiceSpec::service_name(&spu_meta.name);
            if let Some(svc) = self.services.store().value(&svc_name).await {
                self.apply_ingress_from_svc(spu_md, svc.inner_owned())
                    .await?;
            } else {
                debug!("no svc exists for spu {}, skipping", spu_id);
            }
        }

        Ok(())
    }

    /// svc has been changed, update spu
    async fn sync_from_spu_services(
        &mut self,
        listener: &mut K8ChangeListener<SpuServiceSpec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no service change, skipping");
            return Ok(());
        }

        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received spu service changes updates: {}, deletes: {}, epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        for svc_md in updates.into_iter() {
            let svc_id = svc_md.key();
            let svc_meta = svc_md.ctx().item().inner();
            if let Some(spu_name) = SpuServiceSpec::spu_name(&svc_meta) {
                if let Some(spu) = self.spus.store().value(spu_name).await {
                    self.apply_ingress_from_svc(spu.inner_owned(), svc_md)
                        .await?;
                } else {
                    debug!("no spu exists for svc {}, skipping", svc_id);
                }
            } else {
                error!(
                    svc = %svc_id,
                    "spu service doesnt have spu name",
                );
            }
        }

        Ok(())
    }

    async fn apply_ingress_from_svc(
        &mut self,
        spu_md: MetadataStoreObject<SpuSpec, K8MetaItem>,
        svc_md: MetadataStoreObject<SpuServiceSpec, K8MetaItem>,
    ) -> Result<(), ClientError> {
        let spu_id = spu_md.key();
        let svc_id = svc_md.key();

        // Check what type of load balancing we're using between NodePort and LoadBalancer
        //debug!("Applying ingress address based on services");
        //debug!("SPU: {:?}", &spu_md);
        //debug!("SVC: {:?}", &svc_md);

        // Get the current ingress on the spu
        let spu_ingress = spu_md.spec.public_endpoint.ingress.clone();
        let spu_port = spu_md.spec.public_endpoint.port;

        let spu_ingressport = IngressPort {
            port: spu_port,
            ingress: spu_ingress,
            ..Default::default()
        };

        // Get the external ingress from the service
        // Look at svc_md to identify if LoadBalancer
        let lb_type = svc_md.spec().inner().r#type.as_ref();
        let spu_svc_port = svc_md.spec().inner().ports[0].port;
        let spu_svc_nodeport = svc_md.spec().inner().ports[0]
            .node_port
            .ok_or_else(|| ClientError::Other("SPU service missing NodePort".into()))?;

        // This will either have a value from External-IP, or will be empty
        // If empty, the use `fluvio.io/ingress-address` annotation to set an external address for SPU
        let svc_lb_ingresses = svc_md.status.ingress();

        // Choose the external port based on the service type
        let mut computed_spu_ingressport = match lb_type {
            Some(LoadBalancerType::NodePort) => IngressPort {
                port: spu_svc_nodeport,
                ..Default::default()
            },
            _ => IngressPort {
                port: spu_svc_port,
                ingress: svc_lb_ingresses.iter().map(convert).collect(),
                ..Default::default()
            },
        };

        // Add additional ingress via annotation value
        add_ingress_from_svc_annotation(&svc_md, &mut computed_spu_ingressport.ingress);

        debug!(
            "Computed SPU ingress after applying any svc annotation: {:?}",
            &computed_spu_ingressport
        );

        // We're going to come in with a fully built IngressPort
        if computed_spu_ingressport != spu_ingressport {
            let mut update_spu = spu_md.spec.clone();
            debug!(
                "updating spu: {} public end point: {:#?} from svc: {}",
                spu_id, computed_spu_ingressport, svc_id
            );
            update_spu.public_endpoint.ingress = computed_spu_ingressport.ingress;
            update_spu.public_endpoint.port = computed_spu_ingressport.port;
            self.spus.create_spec(spu_id.to_owned(), update_spu).await?;
        } else {
            debug!(
                "detected no spu: {} ingress changes with svc: {}",
                spu_id, svc_id
            );
        };

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

                // assume that if spu exists, it will have necessary attribute for now
                self.apply_spu(&spg_obj, &spu_name, spu_id).await?;
            }
        }

        Ok(())
    }

    #[allow(clippy::ptr_arg)]
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

        let spu_count = self.spus.store().count().await;
        debug!(%spu_name,spu_count,"applying spu");
        trace!("spu action: {:#?}", action);
        self.spus.wait_action(spu_name, action).await?;
        let spu_count = self.spus.store().count().await;
        debug!(spu_count, "finished applying spu");

        Ok(())
    }
}

fn add_ingress_from_svc_annotation(
    svc_md: &MetadataStoreObject<SpuServiceSpec, K8MetaItem>,
    computed_spu_ingress: &mut Vec<IngressAddr>,
) {
    debug!("Checking ingress for annotations: {:#?}", &svc_md);
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

/// compute spu id with min_id as base
fn compute_spu_id(min_id: i32, replica_index: u16) -> i32 {
    replica_index as i32 + min_id
}
