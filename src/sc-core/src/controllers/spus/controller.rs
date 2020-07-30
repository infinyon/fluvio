//!
//! # Spu Controller

use std::collections::HashMap;
use std::time::Instant;
use std::time::Duration;

use log::debug;
use log::error;
use log::warn;

use async_channel::Receiver;

use flv_future_aio::task::spawn;
use flv_types::SpuId;

use crate::stores::actions::WSAction;
use crate::core::SharedContext;
use crate::stores::StoreContext;
use crate::stores::spu::*;

use super::SpuAction;

struct SpuOnlineStatus {
    online: bool,
    // last time status was known
    time: Instant,
}

impl SpuOnlineStatus {
    fn new() -> Self {
        Self {
            online: false,
            time: Instant::now(),
        }
    }
}

/// Keep track of SPU status
/// if SPU has not send heart beat within a period, it is considered down
pub struct SpuController {
    spus: StoreContext<SpuSpec>,
    health_receiver: Receiver<SpuAction>,
    status: HashMap<SpuId, SpuOnlineStatus>,
}

impl SpuController {
    pub fn start(ctx: SharedContext) {
        let controller = Self {
            spus: ctx.spus().clone(),
            health_receiver: ctx.health().receiver(),
            status: HashMap::new(),
        };

        spawn(async move {
            controller.dispatch_loop().await;
        });
    }

    async fn dispatch_loop(mut self) {
        use tokio::select;
        use flv_future_aio::timer::sleep;

        self.sync_store().await;

        const HEALTH_DURATION: u64 = 90;

        let mut time_left = Duration::from_secs(HEALTH_DURATION);
        loop {
            let health_time = Instant::now();
            debug!(
                "waiting on events, health check left: {} secs",
                time_left.as_secs()
            );

            select! {
                _ = self.spus.listen() => {

                    debug!("detected events in spu store");
                    self.sync_store().await;
                    time_left = time_left - health_time.elapsed();
                },
                health_msg = self.health_receiver.recv() => {

                    match health_msg {
                        Ok(health) => {
                            debug!("received health message: {:?}",health);
                            self.send_spu_status(health).await;
                        },
                        Err(err) => {
                            error!("error receiving health msg: {}",err);
                        }
                    }
                    time_left = time_left - health_time.elapsed();

                },
                _ = sleep(time_left) => {
                    self.health_check().await;
                    time_left = Duration::from_secs(HEALTH_DURATION);
                },

            }
        }
    }

    /// sync spu status with store
    async fn sync_store(&mut self) {
        use std::collections::HashSet;

        // check if we need to sync spu and our health check cache
        if self.spus.store().count().await as usize != self.status.len() {
            let keys = self.spus.store().spu_ids().await;
            let mut status_keys: HashSet<SpuId> =
                self.status.keys().map(|key| key.clone()).collect();
            debug!(
                "syncing store before store: {} items, health status: {} items",
                keys.len(),
                status_keys.len()
            );
            for spu_id in &keys {
                if self.status.contains_key(spu_id) {
                    status_keys.remove(spu_id);
                } else {
                    self.status.insert(*spu_id, SpuOnlineStatus::new());
                }
            }

            for spu_id in status_keys {
                self.status.remove(&spu_id);
            }

            assert_eq!(self.status.len(), keys.len());
            debug!("syncing store after status: {} items", self.status.len());
        }

        // check if any of spu are init, change to offline
        let read_guard = self.spus.store().read().await;
        let spu_names: Vec<String> = read_guard
            .values()
            .filter_map(|spu| {
                if spu.status.resolution == SpuStatusResolution::Init {
                    Some(spu.key.clone())
                } else {
                    None
                }
            })
            .collect();

        drop(read_guard);

        for spu_name in spu_names.into_iter() {
            if let Err(err) = self
                .spus
                .send(vec![WSAction::UpdateStatus((
                    spu_name.clone(),
                    SpuStatus::offline(),
                ))])
                .await
            {
                error!("error sending spu status: {}", err);
            } else {
                debug!("set spu: {} to offline", spu_name);
            }
        }
    }

    async fn send_spu_status(&mut self, action: SpuAction) {
        // first check if spu cache exists
        if let Some(cache_status) = self.status.get_mut(&action.id) {
            cache_status.time = Instant::now();
            cache_status.online = action.status;

            self.send_health_to_store(vec![action]).await;
        } else {
            warn!("not finding cache status: {}", action.id);
        }
    }

    /// check if any spu has done health check
    async fn health_check(&mut self) {
        debug!("performing health check");
        let mut actions: Vec<SpuAction> = vec![];
        for (id, status) in self.status.iter_mut() {
            let elapsed_time = status.time.elapsed();
            if elapsed_time >= Duration::from_secs(70) {
                debug!(
                    "spu: {}, no health check: {} seconds",
                    id,
                    elapsed_time.as_secs()
                );
                status.online = false;
                status.time = Instant::now();
                actions.push(SpuAction::down(*id));
            }
        }

        if actions.len() > 0 {
            debug!("health check: {} spus needed to be offline", actions.len());
            self.send_health_to_store(actions).await;
        } else {
            debug!("health check: everything is up to date, no actions needed");
        }
    }

    // send health status to store
    async fn send_health_to_store(&mut self, actions: Vec<SpuAction>) {
        for action in actions.into_iter() {
            if let Some(mut spu) = self.spus.store().get_by_id(action.id).await {
                let mut update = false;
                if action.status && spu.status.is_offline() {
                    spu.status.set_online();
                    update = true;
                } else if !action.status && spu.status.is_online() {
                    spu.status.set_offline();
                    update = true;
                }

                if update {
                    if let Err(err) = self
                        .spus
                        .send(vec![WSAction::UpdateStatus((
                            spu.key_owned(),
                            spu.status.clone(),
                        ))])
                        .await
                    {
                        error!("error sending spu status: {}", err);
                    } else {
                        debug!("send spu: {}, status: {} to store", action.id, spu.status);
                    }
                } else {
                    debug!("spu: {} health, no change", action.id);
                }
            } else {
                warn!("unknown spu id: {:?}", action);
            }
        }
    }

    /*
    /// process requests related to SPU management
    async fn process_request(&mut self, request: SpuChangeRequest) {
        // process SPU action; update context with new actions
        match self.spu_reducer.process_requests(request).await {
            Ok(actions) => {
                debug!("SPU Controller apply actions: {}", actions);
                // send actions to kv
                if actions.spus.len() > 0 {
                    for ws_action in actions.spus.into_iter() {
                        //  log_on_err!(self.ws_service.update_spu(ws_action).await);
                    }
                }
                /*
                if actions.conns.len() > 0 {
                    self.conn_manager.process_requests(actions.conns).await;
                }
                */
            }
            Err(err) => error!("error generating spu actions from reducer: {}", err),
        }
    }
    */
}

/*
#[cfg(test)]
mod tests {

    use log::debug;
    use futures::channel::mpsc::channel;
    use futures::channel::mpsc::Receiver;

    use flv_future_core::test_async;
    use utils::actions::Actions;
    use flv_metadata_cluster::spu::SpuSpec;

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
