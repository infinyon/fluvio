//!
//! # Spu Controller

use std::time::Instant;
use std::time::Duration;

use tracing::{debug, error, instrument};

use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::StoreContext;
use crate::stores::spu::*;

/// Reconcile SPU health status with Meta data
/// if SPU has not send heart beat within a period, it is considered down
pub struct SpuController {
    spus: StoreContext<SpuSpec>,
    health_check: SharedHealthCheck,
}

impl SpuController {
    pub fn start(ctx: SharedContext) {
        let controller = Self {
            spus: ctx.spus().clone(),
            health_check: ctx.health().clone(),
        };

        debug!("starting spu controller");
        spawn(async move {
            controller.inner_loop().await;
        });
    }

    #[instrument(skip(self))]
    async fn inner_loop(mut self) {
        use tokio::select;
        use fluvio_future::timer::sleep;

        let mut spu_listener = self.spus.change_listener();
        let _ = spu_listener.wait_for_initial_sync().await;

        let mut health_listener = self.health_check.listener();

        const HEALTH_DURATION: u64 = 90;

        let mut time_left = Duration::from_secs(HEALTH_DURATION);
        loop {
            self.sync_store().await;
            let health_time = Instant::now();
            debug!(
                "waiting on events, health check left: {} secs",
                time_left.as_secs()
            );

            select! {
                _ = spu_listener.listen() => {
                    debug!("detected events in spu store");
                    spu_listener.load_last();
                    time_left -= health_time.elapsed();

                },
                _ = health_listener.listen() => {
                    debug!("heal check events");
                    //health_listener.load_last();
                    time_left -= health_time.elapsed();

                },
                _ = sleep(time_left) => {
                  //  self.health_check().await;
                    time_left = Duration::from_secs(HEALTH_DURATION);
                },

            }
        }
    }

    /// sync spu status with store
    #[instrument(skip(self))]
    async fn sync_store(&mut self) {
        // first get status values
        let spus = self.spus.store().clone_values().await;

        let mut changes = vec![];
        let health_read = self.health_check.read().await;

        for mut spu in spus.into_iter() {
            // check if we need to sync spu and our health check cache

            let spu_id = spu.spec.id;

            match health_read.get(&spu_id) {
                Some(health_status) => {
                    if spu.status.is_init() || spu.status.is_online() != *health_status {
                        if *health_status {
                            spu.status.set_online();
                        } else {
                            spu.status.set_offline();
                        }
                        debug!(id = spu.spec.id, status = %spu.status,"spu status changed");
                        changes.push(spu);
                    }
                }
                None => {}
            }
        }

        drop(health_read);

        for updated_spu in changes.into_iter() {
            let key = updated_spu.key;
            let status = updated_spu.status;
            if let Err(err) = self.spus.update_status(key, status).await {
                error!("error updating status: {:#?}", err);
            }
        }
    }
}

/*
#[cfg(test)]
mod tests {

    use tracing::debug;
    use futures::channel::mpsc::channel;
    use futures::channel::mpsc::Receiver;

    use flv_future_core::test_async;
    use utils::actions::Actions;
    use fluvio_controlplane_metadata::spu::SpuSpec;

    use crate::cli::ScConfig;
    use crate::core::ScMetadata;
    use crate::core::ScContext;
    use crate::core::ScRequest;
    use crate::core::auth_tokens::AuthTokenAction;
    use crate::core::spus::SpuAction;
    use crate::core::spus::SpuKV;
    use crate::tests::fixture::MockConnectionManager;
    use crate::tests::fixture::SharedKVStore;
    use crate::core::send_channels::ScSendChannels;
    use crate::core::common::new_channel;
    use crate::hc_manager::HcAction;

    use super::ScController;

    fn sample_controller() -> (
        ScController<SharedKVStore, MockConnectionManager>,
        (Receiver<ScRequest>, Receiver<Actions<HcAction>>),
    ) {
        let default_config = ScConfig::default();
        let metadata = ScMetadata::shared_metadata(default_config);
        let conn_manager = MockConnectionManager::shared_conn_manager();

        let (sc_sender, sc_receiver) = channel::<ScRequest>(100);
        let (hc_sender, hc_receiver) = new_channel::<Actions<HcAction>>();
        let kv_store = SharedKVStore::new(sc_sender.clone());
        let sc_send_channels = ScSendChannels::new(hc_sender.clone());

        (
            ScController::new(
                ScContext::new(sc_send_channels, conn_manager.clone()),
                metadata.clone(),
                kv_store.clone(),
            ),
            (sc_receiver, hc_receiver),
        )
    }

    /// test add new spu
    #[test_async]
    async fn test_controller_basic() -> Result<(), ()> {
        let (mut controller, _other) = sample_controller();

        let auth_token_actions: Actions<AuthTokenAction> = Actions::default();

        let spu: SpuKV = SpuSpec::new(5000).into();
        let mut spu_actions: Actions<SpuAction> = Actions::default();

        spu_actions.push(SpuAction::AddSpu(spu.spec().name(), spu.clone()));
        /*
        let topic_actions: Actions<TopicAction> = Actions::default();
        for (name,topic) in self.kv.topics.read().iter() {
            topic_actions.push(TopicAction::AddTopic(name.clone(),topic.clone()));
        }

        let partition_actions: Actions<PartitionAction> = Actions::default();
        for (key,partition) in self.kv.partitions.read().iter() {
            partition_actions.push(PartitionAction::AddPartition(key.clone(),partition.clone()));
        }
        */
        controller
            .process_sc_request(ScRequest::UpdateAll(
                auth_token_actions,
                spu_actions,
                Actions::default(),
                Actions::default(),
            ))
            .await;

        let metadata = controller.metadata();
        debug!("metadata: {:#?}", metadata);

        // metdata should container new spu
        assert!(metadata.spus().spu("spu-5000").is_some());

        Ok(())
    }

}
*/
