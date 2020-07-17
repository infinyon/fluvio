use std::sync::Arc;

use log::debug;
use log::error;
use log::info;
use log::trace;
use futures::stream::StreamExt;

use flv_future_aio::task::spawn;
use k8_client::ClientError;
use flv_metadata::k8::metadata::*;
use flv_metadata::k8::core::service::*;
use k8_client::metadata::MetadataClient;
use k8_client::SharedK8Client;
use flv_metadata::spu::IngressAddr;


use flv_sc_core::stores::spu::SpuAdminStore;
use crate::ScK8Error;

/// An operator to deal with Svc
/// It is used to update SPU's public ip address from external load balancer service.
/// External load balancer update external ip or hostname out of band.
pub struct SvcOperator {
    client: SharedK8Client,
    spu_store: Arc<SpuAdminStore>,
    namespace: String,
}

impl SvcOperator {
    pub fn run(
        client: SharedK8Client,
        namespace: String,
        spu_store: Arc<SpuAdminStore>,
    )  {
       
       let operator =  Self {
            client,
            namespace,
            spu_store,
        };

        spawn(operator.outer_loop());
    }

    async fn outer_loop(mut self) {
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

    async fn dispatch_events(&self, events: Vec<Result<K8Watch<ServiceSpec>, ClientError>>) {
        for event_r in events {
            match event_r {
                Ok(watch_event) => {
                    let result = self.process_event(watch_event).await;
                    match result {
                        Err(err) => error!("error processing k8 service event: {}", err),
                        _ => {}
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

    async fn apply_svc_changes(&self, svc_obj: K8Obj<ServiceSpec>) -> Result<(), ScK8Error> {
        debug!("received svc: {}",svc_obj.metadata.name);
        trace!("svc spec: {:#?}", svc_obj);

        if let Some(spu_id) = svc_obj.metadata.labels.get("fluvio.io/spu-name") {
            if let Some(mut old_value) = self.spu_store.value(spu_id).await {

                debug!(
                    "found spu: {} to update ingress from external load balancer ",
                    spu_id
                );

                let ingresses = svc_obj.status.load_balancer.ingress;
                old_value.spec.public_endpoint.ingress =
                    ingresses.into_iter().map(|addr| convert(addr)).collect();
               // self.k8_ws.update_spec(old_value.inner_owned()).await?;
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
