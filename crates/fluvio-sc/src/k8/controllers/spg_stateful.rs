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
        let _ = spg_listener.wait_for_initial_sync().await;

        let mut config_listener = self.configs.change_listener();
        let _ = config_listener.wait_for_initial_sync().await;

        debug!("initial sync has been done");

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
            debug!("no spg change, skipping");
            return Ok(());
        }

        debug!("start syncing spg");

        let changes = listener.sync_changes().await;
        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();

        debug!(
            "received spg changes updates: {},deletes: {},epoch: {}",
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
            error!("fluvio config map not found, skipping");
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
            let status = SpuGroupStatus::invalid(format!("conflict with: {conflict_id}"));

            self.groups
                .update_status(spg_name.to_owned(), status)
                .await?;
        } else {
            // if we pass this stage, then we reserved id.
            if !spu_group.is_already_valid() {
                debug!("not validated, setting to reserve status");
                let status = SpuGroupStatus::reserved();
                self.groups
                    .update_status(spg_name.to_owned(), status)
                    .await?;
                return Ok(());
            }

            debug!("spg group is valid, generating statefulset");
            let (spg_service_key, spg_service_action) = spu_group.as_service();

            debug!("starting spg service generation");
            trace!("spg_service_actions: {:#?}", spg_service_action);
            self.spg_services
                .wait_action(&spg_service_key, spg_service_action)
                .await?;

            debug!("starting stateful set generation");

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

#[cfg(test)]
mod test {

    use tracing::debug;

    use fluvio_stream_dispatcher::actions::WSAction;
    use fluvio_stream_dispatcher::dispatcher::K8ClusterStateDispatcher;

    use crate::k8::fixture::TestEnv;
    use super::*;

    //
    // apiVersion: v1
    // data:
    //   image: infinyon/fluvio:b9281e320266e295e75200d10b769967e23d3ed2
    // lbServiceAnnotations: '{"fluvio.io/ingress-address":"192.168.208.2"}'
    // podSecurityContext: '{}'
    // service: '{"type":"NodePort"}'
    // spuPodConfig: '{"nodeSelector":{},"resources":{"limits":{"memory":"1Gi"},"requests":{"memory":"256Mi"}},"storageClass":null}'
    //kind: ConfigMap

    #[fluvio_future::test(ignore)]
    async fn test_statefulset() {
        let test_env = TestEnv::create().await;
        let (global_ctx, config_ctx) = test_env.create_global_ctx().await;

        let statefulset_ctx: StoreContext<StatefulsetSpec> = StoreContext::new();
        let spg_service_ctx: StoreContext<SpgServiceSpec> = StoreContext::new();

        // start statefullset dispatcher
        K8ClusterStateDispatcher::<_, _>::start(
            test_env.ns().to_owned(),
            test_env.client().clone(),
            statefulset_ctx.clone(),
        );

        // start spg service dispatcher
        K8ClusterStateDispatcher::<_, _>::start(
            test_env.ns().to_owned(),
            test_env.client().clone(),
            spg_service_ctx.clone(),
        );

        K8ClusterStateDispatcher::<_, _>::start(
            test_env.ns().to_owned(),
            test_env.client().clone(),
            global_ctx.spgs().clone(),
        );

        SpgStatefulSetController::start(
            test_env.ns().to_owned(),
            config_ctx.clone(),
            global_ctx.spgs().clone(),
            statefulset_ctx,
            global_ctx.spus().clone(),
            spg_service_ctx,
            None,
        );

        // wait for controllers to startup
        sleep(Duration::from_millis(10)).await;

        // create spu group
        let spg_name = "test".to_string();
        let spg_spec = SpuGroupSpec {
            replicas: 1,
            ..Default::default()
        };
        global_ctx
            .spgs()
            .wait_action(
                &spg_name,
                WSAction::UpdateSpec((spg_name.clone(), spg_spec)),
            )
            .await
            .expect("create");

        debug!("spu group has been created");

        sleep(Duration::from_secs(10)).await;

        //test_env.delete().await;
    }
}
