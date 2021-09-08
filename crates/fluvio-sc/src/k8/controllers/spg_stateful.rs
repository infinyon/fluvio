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

#[cfg(test)]
mod test {

    use std::iter;

    use tracing::debug;

    use k8_metadata_client::MetadataClient;
    use k8_types::core::namespace::NamespaceSpec;
    use k8_types::{InputK8Obj, InputObjectMeta};
    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;

    use k8_client::{K8Client, SharedK8Client, load_and_share};

    use super::*;

    struct TestEnv {
        ns: String,
        client: SharedK8Client
    }

    impl TestEnv {

        async fn create() -> Self {

            let client = load_and_share().expect("creating k8 client");
            let ns = Self::create_unique_ns();
            Self::create_ns(&ns, &client).await;

            Self {
                ns,
                client
            }
        }

        fn create_unique_ns() -> String {
            /* 
            let mut rng = thread_rng();
            let ns: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(7)
                .collect();
            ns
            */
            "test2".to_owned()
        }

        async fn create_ns(ns: &str,k8_client: &K8Client)  {
        
            let input_meta = InputObjectMeta {
                name: ns.to_owned(),
                ..Default::default()
            };

            debug!(%ns,"creating ns");
            let input = InputK8Obj::new(NamespaceSpec::default(), input_meta);
            k8_client.apply(input).await.expect("ns created");
            
        }
    }

    #[fluvio_future::test(ignore)]
    async fn test_statefulset() {
        let _test_env = TestEnv::create().await;

        // create unique ns
        /*
        SpgStatefulSetController::start(
            namespace,
            config_ctx.clone(),
            global_ctx.spgs().clone(),
            statefulset_ctx,
            global_ctx.spus().clone(),
            spg_service_ctx,
            tls,
        );
        */
    }
}
