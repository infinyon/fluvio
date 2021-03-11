use std::{collections::HashMap, fmt, time::Duration};

use fluvio_controlplane_metadata::store::MetadataStoreObject;
use fluvio_stream_dispatcher::actions::WSAction;
use k8_client::{ClientError, SharedK8Client};
use tracing::debug;
use tracing::trace;
use tracing::error;
use tracing::instrument;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use k8_types::core::service::LoadBalancerIngress;

use crate::stores::spg::{SpuGroupSpec};
use crate::stores::{StoreContext, K8ChangeListener};
use crate::k8::objects::spu_k8_config::ScK8Config;
use crate::stores::spu::IngressAddr;
use crate::stores::spu::SpuSpec;
use crate::k8::objects::spg_group::SpuGroupObj;
use crate::k8::objects::spu_service::SpuServicespec;

/// Manages SpuService
/// It is used to update SPU's public ip address from external load balancer service.
/// External load balancer update external ip or hostname out of band.
pub struct SpuServiceController {
    client: SharedK8Client,
    namespace: String,
    services: StoreContext<SpuServicespec>,
    spus: StoreContext<SpuSpec>,
    groups: StoreContext<SpuGroupSpec>,
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
    pub fn start(
        client: SharedK8Client,
        namespace: String,
        spus: StoreContext<SpuSpec>,
        services: StoreContext<SpuServicespec>,
        groups: StoreContext<SpuGroupSpec>,
    ) {
        let controller = Self {
            client,
            namespace,
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

        let mut spu_listener = self.spus.change_listener();
        let mut spg_listener = self.groups.change_listener();

        self.sync_spu_to_spu_service(&mut spu_listener).await;
        self.sync_with_spg(&mut spg_listener).await?;

        loop {
            trace!("waiting events");

            select! {
                _ = spg_listener.listen() => {
                    debug!("detected spg changes");
                    self.sync_with_spg(&mut spg_listener).await?;
                },
                _ = spu_listener.listen() => {
                    debug!("detected spu changes");
                    self.sync_spu_to_spu_service(&mut spu_listener).await;
                }
            }
        }
    }

    #[instrument()]
    /// spu has been changed, sync with existing services
    async fn sync_spu_to_spu_service(&mut self, listener: &mut K8ChangeListener<SpuSpec>) {
        if !listener.has_change() {
            debug!("no spu changes, skipping");
            return;
        }

        let changes = listener.sync_changes().await;

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

        let spu_k8_config = ScK8Config::load(&self.client, &self.namespace).await?;

        for group_item in updates.into_iter() {
            let spg_obj = SpuGroupObj::new(group_item);

            let spec = spg_obj.spec();
            let replicas = spec.replicas;
            for i in 0..replicas {
                
                let spu_name = format!("{}-{}", spg_obj.key(), i);
                debug!(%spu_name,"generating spu with name");

                self.apply_spu_load_balancers(&spg_obj, &spu_name, &spu_k8_config)
                .await?;
            }
        }

        Ok(())
    }

    async fn apply_spu_load_balancers(
        &self,
        spg_obj: &SpuGroupObj,
        spu_name: &str,
        spu_k8_config: &ScK8Config,
    ) -> Result<(), ClientError> {
        use fluvio_types::defaults::{SPU_PUBLIC_PORT};
        use k8_types::core::service::{ServicePort, ServiceSpec as K8ServiceSpec, TargetPort};

        let mut public_port = ServicePort {
            port: SPU_PUBLIC_PORT,
            ..Default::default()
        };
        public_port.target_port = Some(TargetPort::Number(public_port.port));

        let mut selector = HashMap::new();
        let pod_name = format!("fluvio-spg-{}", spu_name);
        selector.insert("statefulset.kubernetes.io/pod-name".to_owned(), pod_name);

        let mut k8_service_spec = K8ServiceSpec {
            selector: Some(selector),
            ports: vec![public_port.clone()],
            ..Default::default()
        };

        spu_k8_config.apply_service(&mut k8_service_spec);

        let svc_name = format!("fluvio-spu-{}", spu_name);

        let action = WSAction::Apply(
            MetadataStoreObject::with_spec(svc_name.clone(), k8_service_spec.into()).with_context(
                spg_obj
                    .ctx()
                    .create_child()
                    .set_labels(vec![("fluvio.io/spu-name", spu_name)]),
            ),
        );

        trace!("spu action: {:#?}", action);

        self.services.wait_action(&svc_name, action).await?;

        Ok(())
    }
}

fn convert(ingress_addr: &LoadBalancerIngress) -> IngressAddr {
    IngressAddr {
        hostname: ingress_addr.hostname.clone(),
        ip: ingress_addr.ip.clone(),
    }
}
