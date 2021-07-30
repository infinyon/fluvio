use std::{time::Duration};

use fluvio_stream_dispatcher::{store::K8ChangeListener};
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
use tracing::instrument;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use k8_client::SharedK8Client;
use k8_client::ClientError;

use crate::stores::{StoreContext};
use crate::stores::spg::{SpuGroupSpec, SpuGroupStatus};
use crate::stores::spu::{SpuSpec};
use crate::cli::TlsConfig;

use crate::k8::objects::spg_group::{SpuGroupObj};
use crate::k8::objects::spu_k8_config::ScK8Config;
use crate::k8::objects::statefulset::StatefulsetSpec;
use crate::k8::objects::spg_service::SpgServiceSpec;

/// reconcile between SPG and Statefulset
pub struct SpgStatefulSetController {
    client: SharedK8Client,
    namespace: String,
    groups: StoreContext<SpuGroupSpec>,
    spus: StoreContext<SpuSpec>,
    statefulsets: StoreContext<StatefulsetSpec>,
    spg_services: StoreContext<SpgServiceSpec>,
    tls: Option<TlsConfig>,
}

impl SpgStatefulSetController {
    pub fn start(
        client: SharedK8Client,
        namespace: String,
        groups: StoreContext<SpuGroupSpec>,
        statefulsets: StoreContext<StatefulsetSpec>,
        spus: StoreContext<SpuSpec>,
        spg_services: StoreContext<SpgServiceSpec>,
        tls: Option<TlsConfig>,
    ) {
        let controller = Self {
            client,
            namespace,
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

    #[instrument(skip(self), name = "SpgLoop")]
    async fn inner_loop(&mut self) -> Result<(), ClientError> {
        use tokio::select;

        let mut spg_listener = self.groups.change_listener();

        loop {
            self.sync_spgs_to_statefulset(&mut spg_listener).await?;

            select! {
                _ = spg_listener.listen() => {
                    debug!("detected spg changes");
                },

            }
        }
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

        // load k8 config,
        let spu_k8_config = ScK8Config::load(&self.client, &self.namespace).await?;

        for group_item in updates.into_iter() {
            let spu_group = SpuGroupObj::new(group_item);

            self.sync_spg_to_statefulset(spu_group, &spu_k8_config)
                .await?
        }

        Ok(())
    }

    #[instrument(skip(self, spu_k8_config))]
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
                let status = SpuGroupStatus::reserved();
                self.groups
                    .update_status(spg_name.to_owned(), status)
                    .await?;
                return Ok(());
            }

            let (spg_service_key, spg_service_action) = spu_group.generate_service();

            trace!("spg_service_actions: {:#?}", spg_service_action);
            self.spg_services
                .wait_action(&spg_service_key, spg_service_action)
                .await?;

            let (stateful_key, stateful_action) =
                spu_group.statefulset_action(&self.namespace, spu_k8_config, self.tls.as_ref());

            debug!(?stateful_action, "applying statefulset");
            self.statefulsets
                .wait_action(&stateful_key, stateful_action)
                .await?;
        }

        Ok(())
    }
}
