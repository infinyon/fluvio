

use log::debug;
use log::error;
use log::info;
use log::trace;
use futures::stream::StreamExt;
use pin_utils::pin_mut;

use future_helper::spawn;
use k8_client::ClientError;
use k8_metadata::core::metadata::K8Watch;
use k8_metadata::core::metadata::K8Obj;
use k8_client::service::ServiceSpec;
use k8_client::service::ServiceStatus;
use k8_client::service::LoadBalancerIngress;
use metadata::spu::IngressAddr;

use crate::k8::K8WSUpdateService;
use crate::core::spus::SharedSpuLocalStore;
use crate::ScServerError;

/// An operator to deal with Svc
/// It is used to update SPU's public ip address from external load balancer service.
/// External load balancer update external ip or hostname out of band.
pub struct SvcOperator {
    k8_ws: K8WSUpdateService,
    spu_store: SharedSpuLocalStore,
    namespace: String
}



impl SvcOperator {

    pub fn new(
            k8_ws: K8WSUpdateService,
            namespace: String,
            spu_store: SharedSpuLocalStore) -> Self {
        Self {
            k8_ws,
            namespace,
            spu_store
        }
    }

    pub fn run(self) {
        spawn(self.inner_run());
    }


     async fn inner_run(self)  {

        let svc_stream = self.k8_ws.client().watch_stream_since::<ServiceSpec>(&self.namespace, None);
        pin_mut!(svc_stream);

        info!("starting svc operator with namespace: {}",self.namespace);
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

    async fn dispatch_events(&self,events: Vec<Result<K8Watch<ServiceSpec,ServiceStatus>, ClientError>>) {
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

    async fn process_event(&self,event: K8Watch<ServiceSpec,ServiceStatus>) -> Result<(), ScServerError> {
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

    async fn apply_svc_changes(&self,
        svc_obj: K8Obj<ServiceSpec,ServiceStatus>,
    ) -> Result<(), ScServerError> {

        debug!("svc spec: {:#?}",svc_obj);

        if let Some(spu_id) = svc_obj.metadata.labels.get("fluvio.io/spu-name") {
            if let Some(mut old_value) = self.spu_store.value(spu_id) {
                debug!("found spu: {} to update ingress from external load balancer ",spu_id);
                if let Some(status) = svc_obj.status {
                    let ingresses = status.load_balancer.ingress;
                    old_value.spec.public_endpoint.ingress = ingresses.into_iter().map(|addr| convert(addr)).collect(); 
                    self.k8_ws.update_spec(old_value).await?;
                }
            }
        }
        Ok(())
    }

}

fn convert(ingress_addr: LoadBalancerIngress) -> IngressAddr {
    IngressAddr {
        hostname: ingress_addr.hostname,
        ip: ingress_addr.ip
    }
}
