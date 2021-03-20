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

use crate::stores::spg::{SpuGroupSpec};
use crate::stores::{StoreContext, K8ChangeListener};
use crate::k8::objects::spu_k8_config::ScK8Config;
use crate::k8::objects::spg_group::SpuGroupObj;
use crate::k8::objects::spu_service::SpuServiceSpec;

/// Manages SpuService
/// It is used to update SPU's public ip address from external load balancer service.
/// External load balancer update external ip or hostname out of band.
pub struct SpuServiceController {
    client: SharedK8Client,
    namespace: String,
    services: StoreContext<SpuServiceSpec>,
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
        services: StoreContext<SpuServiceSpec>,
        groups: StoreContext<SpuGroupSpec>,
    ) {
        let controller = Self {
            client,
            namespace,
            services,
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

        let mut spg_listener = self.groups.change_listener();

        self.sync_with_spg(&mut spg_listener).await?;

        loop {
            trace!("waiting events");

            select! {
                _ = spg_listener.listen() => {
                    debug!("detected spg changes");
                    self.sync_with_spg(&mut spg_listener).await?;
                },

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

        let svc_name = SpuServiceSpec::service_name(spu_name);

        let mut ctx = spg_obj
            .ctx()
            .create_child()
            .set_labels(vec![("fluvio.io/spu-name", spu_name)]);
        ctx.item_mut().annotations = spu_k8_config.lb_service_annotations.clone();

        let obj = MetadataStoreObject::with_spec(svc_name.clone(), k8_service_spec.into())
            .with_context(ctx);
        debug!("action: {:#?}", obj);
        let action = WSAction::Apply(obj);

        self.services.wait_action(&svc_name, action).await?;

        Ok(())
    }
}
