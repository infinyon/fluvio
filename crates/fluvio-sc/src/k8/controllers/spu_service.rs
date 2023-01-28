use std::{collections::HashMap, fmt, time::Duration};

use tracing::{debug, error, info, instrument, trace, warn};

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use fluvio_controlplane_metadata::store::MetadataStoreObject;
use fluvio_controlplane_metadata::store::k8::K8MetaItem;
use fluvio_stream_dispatcher::actions::WSAction;
use k8_client::{ClientError};

use crate::stores::spg::{SpuGroupSpec};
use crate::stores::{StoreContext, K8ChangeListener};
use crate::k8::objects::spu_k8_config::ScK8Config;
use crate::k8::objects::spg_group::SpuGroupObj;
use crate::k8::objects::spu_service::SpuServiceSpec;

/// Sync individual SPU services from SPU Group
pub struct SpuServiceController {
    services: StoreContext<SpuServiceSpec>,
    groups: StoreContext<SpuGroupSpec>,
    configs: StoreContext<ScK8Config>,
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
        configs: StoreContext<ScK8Config>,
        services: StoreContext<SpuServiceSpec>,
        groups: StoreContext<SpuGroupSpec>,
    ) {
        let controller = Self {
            configs,
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
        let _ = spg_listener.wait_for_initial_sync().await;
        let mut config_listener = self.configs.change_listener();
        let _ = config_listener.wait_for_initial_sync().await;

        // at this point k8 config should be loaded
        if let Some(config) = self.configs.store().value("fluvio").await {
            info!("k8 config: {:#?}", config.spec);
        } else {
            // this should never happen
            warn!("K8 config not loaded initially");
        }

        self.sync_with_spg(&mut spg_listener).await?;
        self.sync_with_config(&mut config_listener).await?;

        loop {
            trace!("waiting events");

            select! {
                _ = spg_listener.listen() => {
                    debug!("detected spg changes");
                    self.sync_with_spg(&mut spg_listener).await?;
                },

                _ = config_listener.listen() => {
                    debug!("detected config changes");
                    self.sync_with_config(&mut config_listener).await?;
                }

            }
        }
    }

    async fn sync_with_config(
        &mut self,
        listener: &mut K8ChangeListener<ScK8Config>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no config change, skipping");
            return Ok(());
        }

        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received config changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        for config in updates.into_iter() {
            // iterate over all spg
            self.update_services(self.groups.store().clone_values().await, config.spec)
                .await?;
        }

        Ok(())
    }

    async fn sync_with_spg(
        &mut self,
        listener: &mut K8ChangeListener<SpuGroupSpec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no spg changes, skipping");
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

        if let Some(config) = self.configs.store().value("fluvio").await {
            self.update_services(updates, config.inner_owned().spec)
                .await?;
        } else {
            return Err(ClientError::Other("fluvio config not found".to_owned()));
        }

        Ok(())
    }

    /// update spu
    async fn update_services(
        &self,
        updates: Vec<MetadataStoreObject<SpuGroupSpec, K8MetaItem>>,
        config: ScK8Config,
    ) -> Result<(), ClientError> {
        for group_item in updates.into_iter() {
            let spg_obj = SpuGroupObj::new(group_item);

            let spec = spg_obj.spec();
            let replicas = spec.replicas;
            for i in 0..replicas {
                let spu_name = format!("{}-{}", spg_obj.key(), i);
                debug!(%spu_name,"generating spu with name");

                self.spply_spu_load_service(i, &spg_obj, &spu_name, &config)
                    .await?;
            }
        }

        Ok(())
    }

    #[instrument(skip(self, spg_obj, spu_k8_config))]
    async fn spply_spu_load_service(
        &self,
        replica: u16,
        spg_obj: &SpuGroupObj,
        spu_name: &str,
        spu_k8_config: &ScK8Config,
    ) -> Result<(), ClientError> {
        use k8_types::core::service::{ServiceSpec as K8ServiceSpec};

        let mut selector = HashMap::new();
        let pod_name = format!("fluvio-spg-{spu_name}");
        selector.insert("statefulset.kubernetes.io/pod-name".to_owned(), pod_name);

        let mut k8_service_spec = K8ServiceSpec {
            selector: Some(selector),
            ..Default::default()
        };

        spu_k8_config.apply_service(replica, &mut k8_service_spec);

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
