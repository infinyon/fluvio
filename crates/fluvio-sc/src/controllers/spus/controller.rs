//!
//! # Spu Controller

use std::time::Duration;
use std::io::Error as IoError;

use fluvio_future::timer::sleep;
use tracing::{debug, info, error, trace, instrument};

use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::StoreContext;
use crate::stores::spu::*;

/// Reconcile SPU health status with Meta data
/// if SPU has not send heart beat within a period, it is considered down
pub struct SpuController {
    spus: StoreContext<SpuSpec>,
    health_check: SharedHealthCheck,
    counter: u64, // how many time we have been sync
}

impl SpuController {
    pub fn start(ctx: SharedContext) {
        let controller = Self {
            spus: ctx.spus().clone(),
            health_check: ctx.health().clone(),
            counter: 0,
        };

        info!("starting spu controller");
        spawn(controller.dispatch_loop());
    }

    #[instrument(skip(self), name = "SpuControllerLoop")]
    async fn dispatch_loop(self) {
        info!("started");
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with inner loop: {:#?}", err);
                debug!("sleeping 10 seconds try again");
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    #[instrument(skip(self))]
    async fn inner_loop(&self) -> Result<(), IoError> {
        use tokio::select;

        debug!("initializing listeners");
        let mut spu_listener = self.spus.change_listener();
        let _ = spu_listener.wait_for_initial_sync().await;

        let mut health_listener = self.health_check.listener();
        debug!("finished initializing listeners");

        loop {
            self.sync_store().await?;

            select! {
                _ = spu_listener.listen() => {
                    debug!("detected changes in spu store");
                    spu_listener.load_last();

                },
                _ = health_listener.listen() => {
                    debug!("detected changes in health listener");

                },
            }
        }
    }

    /// sync spu status with store
    #[instrument(skip(self),fields(counter=self.counter))]
    async fn sync_store(&self) -> Result<(), IoError> {
        // first get status values
        let spus = self.spus.store().clone_values().await;

        let mut changes = vec![];
        let health_read = self.health_check.read().await;

        for mut spu in spus.into_iter() {
            // check if we need to sync spu and our health check cache

            let spu_id = spu.spec.id;

            // check if we have status
            if let Some(health_status) = health_read.get(&spu_id) {
                // if status is init, we can set health
                if spu.status.is_init() {
                    if *health_status {
                        spu.status.set_online();
                    } else {
                        spu.status.set_offline();
                    }
                    info!(id = spu.spec.id, status = %spu.status,"init => health status change");
                    changes.push(spu);
                } else {
                    // change if health is different
                    let old_status = spu.status.is_online();
                    if old_status != *health_status {
                        if *health_status {
                            spu.status.set_online();
                        } else {
                            spu.status.set_offline();
                        }
                        info!(id = spu.spec.id, status = %spu.status,"update health status");
                        changes.push(spu);
                    } else {
                        trace!(id = spu.spec.id, status = %spu.status,"ignoring health status");
                    }
                }
            }
        }

        drop(health_read);

        for updated_spu in changes.into_iter() {
            let key = updated_spu.key;
            let status = updated_spu.status;
            debug!(id = updated_spu.spec.id, status = %status, "updating spu status");
            self.spus.update_status(key, status).await?;
        }

        Ok(())
    }
}

/*
#[cfg(test)]
mod tests {

    use tracing::debug;
    use futures::channel::mpsc::channel;
    use futures::channel::mpsc::Receiver;

    use flv_future_core::fluvio_future::test;
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
    #[fluvio_future::test]
    async fn test_controller_basic()  {
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

        // metadata should container new spu
        assert!(metadata.spus().spu("spu-5000").is_some());


    }

}
*/
