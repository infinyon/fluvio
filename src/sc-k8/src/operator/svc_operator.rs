use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::instrument;
use futures::stream::StreamExt;

use flv_future_aio::task::spawn;
use k8_client::ClientError;
use flv_metadata_cluster::k8::metadata::*;
use flv_metadata_cluster::k8::core::service::*;
use k8_client::metadata::MetadataClient;
use k8_client::SharedK8Client;
use flv_metadata_cluster::spu::IngressAddr;
use flv_metadata_cluster::spu::SpuSpec;
use flv_sc_core::core::SharedContext;
use flv_sc_core::stores::StoreContext;
use crate::ScK8Error;

/// An operator to deal with Svc
/// It is used to update SPU's public ip address from external load balancer service.
/// External load balancer update external ip or hostname out of band.
pub struct SvcOperator {
    client: SharedK8Client,
    spus: StoreContext<SpuSpec>,
    namespace: String,
}

impl SvcOperator {
    pub fn run(client: SharedK8Client, namespace: String, ctx: SharedContext) {
        let operator = Self {
            client,
            namespace,
            spus: ctx.spus().clone(),
        };

        spawn(operator.outer_loop());
    }

    async fn outer_loop(mut self) {

        use std::time::Duration;
        use flv_future_aio::timer::sleep;

        // wait for 2 seconds so we don't have problem problem syncing with spu store
        // this may go away if this is converted into regular operator
        sleep(Duration::from_millis(2000)).await;
        info!("starting svc operator");
        loop {
            debug!("starting inner loop");
            self.inner_loop().await;
        }
    }

    async fn inner_loop(&mut self) {
        let mut svc_stream = self
            .client
            .watch_stream_since::<ServiceSpec, _>(self.namespace.clone(), None);

        info!("starting svc operator with namespace: {}", self.namespace);
        while let Some(result) = svc_stream.next().await {
            match result {
                Ok(events) => {
                    self.dispatch_events(events).await;
                }
                Err(err) => error!("error occurred during watch: {}", err),
            }
        }

        debug!("svc operator finished");
    }

    #[instrument(
        skip(self, events),
        fields(namespace = &*self.namespace),
    )]
    async fn dispatch_events(&self, events: Vec<Result<K8Watch<ServiceSpec>, ClientError>>) {
        for event_r in events {
            match event_r {
                Ok(watch_event) => {
                    let result = self.process_event(watch_event).await;
                    if let Err(err) = result {
                        error!("error processing k8 service event: {}", err)
                    }
                }
                Err(err) => error!("error in watch item: {}", err),
            }
        }
    }

    async fn process_event(&self, event: K8Watch<ServiceSpec>) -> Result<(), ScK8Error> {
        trace!("watch event: {:#?}", event);
        match event {
            K8Watch::ADDED(obj) => {
                debug!("watch: ADD event -> apply");
                self.apply_svc_changes(obj).await
            }
            K8Watch::MODIFIED(obj) => {
                debug!("watch: MOD event -> apply");
                self.apply_svc_changes(obj).await
            }
            K8Watch::DELETED(_) => {
                debug!("RCVD watch item DEL event -> deleted");
                Ok(())
            }
        }
    }

    #[instrument(
        skip(self, svc_obj),
        fields(svc_name = &*svc_obj.metadata.name),
    )]
    async fn apply_svc_changes(&self, svc_obj: K8Obj<ServiceSpec>) -> Result<(), ScK8Error> {
        debug!("received svc: {}", svc_obj.metadata.name);
        trace!("svc spec: {:#?}", svc_obj);

        if let Some(spu_id) = svc_obj.metadata.labels.get("fluvio.io/spu-name") {
            if let Some(spu_wrapper) = self.spus.store().value(spu_id).await {
                debug!(
                    "found spu: {} to update ingress from external load balancer ",
                    spu_id
                );
                let ingresses = svc_obj.status.load_balancer.ingress;
                let spu = spu_wrapper.inner_owned();
                let key = spu.key_owned();
                let mut updated_spec = spu.spec;
                updated_spec.public_endpoint.ingress = ingresses.into_iter().map(convert).collect();
                self.spus.create_spec(key, updated_spec).await?;
            } else {
                error!("no spu {} to update!!", spu_id);
            }
        }
        Ok(())
    }
}

fn convert(ingress_addr: LoadBalancerIngress) -> IngressAddr {
    IngressAddr {
        hostname: ingress_addr.hostname,
        ip: ingress_addr.ip,
    }
}
