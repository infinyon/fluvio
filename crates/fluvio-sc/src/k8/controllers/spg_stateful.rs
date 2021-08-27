use std::{time::Duration};

use fluvio_stream_dispatcher::{store::K8ChangeListener};
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
use tracing::instrument;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use k8_client::ClientError;

use crate::stores::{StoreContext};
use crate::stores::spg::{SpuGroupSpec, SpuGroupStatus};
use crate::stores::spu::{SpuSpec};
use crate::cli::TlsConfig;

use crate::k8::objects::spg_group::{SpuGroupObj};
use crate::k8::objects::spu_k8_config::ScK8Config;
use crate::k8::objects::statefulset::StatefulsetSpec;
use crate::k8::objects::spg_service::SpgServiceSpec;

/// Update Statefulset and Service from SPG
pub struct SpgStatefulSetController {
    namespace: String,
    groups: StoreContext<SpuGroupSpec>,
    spus: StoreContext<SpuSpec>,
    statefulsets: StoreContext<StatefulsetSpec>,
    spg_services: StoreContext<SpgServiceSpec>,
    configs: StoreContext<ScK8Config>,
    tls: Option<TlsConfig>,
}

impl SpgStatefulSetController {
    pub fn start(
        namespace: String,
        configs: StoreContext<ScK8Config>,
        groups: StoreContext<SpuGroupSpec>,
        statefulsets: StoreContext<StatefulsetSpec>,
        spus: StoreContext<SpuSpec>,
        spg_services: StoreContext<SpgServiceSpec>,
        tls: Option<TlsConfig>,
    ) {
        let controller = Self {
            namespace,
            configs,
            groups,
            spus,
            statefulsets,
            spg_services,
            tls,
        };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with spg loop loop: {:#?}", err);
                debug!("sleeping 1 miniute to try again");
                sleep(Duration::from_secs(60)).await;
            }
        }
    }

    #[instrument(skip(self), name = "SpgStatefulSetController")]
    async fn inner_loop(&mut self) -> Result<(), ClientError> {
        use tokio::select;

        let mut spg_listener = self.groups.change_listener();
        let mut config_listener = self.configs.change_listener();

        self.sync_spgs_to_statefulset(&mut spg_listener).await?;
        self.sync_with_config(&mut config_listener).await?;

        loop {
            select! {
                _ = spg_listener.listen() => {
                    debug!("detected spg changes");
                    self.sync_spgs_to_statefulset(&mut spg_listener).await?;
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
            for group_item in self.groups.store().clone_values().await {
                let spu_group = SpuGroupObj::new(group_item);

                self.sync_spg_to_statefulset(spu_group, &config.spec)
                    .await?
            }
        }

        Ok(())
    }

    /// svc has been changed, update spu
    async fn sync_spgs_to_statefulset(
        &mut self,
        listener: &mut K8ChangeListener<SpuGroupSpec>,
    ) -> Result<(), ClientError> {
        if !listener.has_change() {
            trace!("no spg change, skipping");
            return Ok(());
        }

        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received service changes updates: {},deletes: {},epoch: {}",
            updates.len(),
            deletes.len(),
            epoch,
        );

        if let Some(config_obj) = self.configs.store().value("fluvio").await {
            let config = config_obj.inner_owned().spec;
            for group_item in updates.into_iter() {
                let spu_group = SpuGroupObj::new(group_item);

                self.sync_spg_to_statefulset(spu_group, &config).await?
            }
        } else {
            error!("config map is not loaded, skipping");
        }

        Ok(())
    }

    #[instrument(skip(self, spu_k8_config, spu_group))]
    async fn sync_spg_to_statefulset(
        &mut self,
        spu_group: SpuGroupObj,
        spu_k8_config: &ScK8Config,
    ) -> Result<(), ClientError> {
        let spg_name = spu_group.key();

        // ensure we don't have conflict with existing spu group
        if let Some(conflict_id) = spu_group.is_conflict_with(self.spus.store()).await {
            warn!(conflict_id, "spg is in conflict with existing id");
            let status = SpuGroupStatus::invalid(format!("conflict with: {}", conflict_id));

            self.groups
                .update_status(spg_name.to_owned(), status)
                .await?;
        } else {
            // if we pass this stage, then we reserved id.
            if !spu_group.is_already_valid() {
                debug!("not valid");
                let status = SpuGroupStatus::reserved();
                self.groups
                    .update_status(spg_name.to_owned(), status)
                    .await?;
                return Ok(());
            }

            debug!("continue");
            let (spg_service_key, spg_service_action) = spu_group.as_service();

            trace!("spg_service_actions: {:#?}", spg_service_action);
            self.spg_services
                .wait_action(&spg_service_key, spg_service_action)
                .await?;

            let (stateful_key, stateful_action) =
                spu_group.as_statefulset(&self.namespace, spu_k8_config, self.tls.as_ref());

            debug!(?stateful_action, "applying statefulset");
            self.statefulsets
                .wait_action(&stateful_key, stateful_action)
                .await?;
        }

        Ok(())
    }
}
