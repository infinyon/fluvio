//!
//! # Topic Controller
//!
//! Reconcile Topics

use log::debug;

use flv_future_aio::task::spawn;

use crate::core::SharedContext;
use crate::stores::topic::*;
use crate::stores::spu::*;
use crate::stores::partition::*;
use crate::stores::*;

use super::reducer::TopicReducer;

#[derive(Debug)]
pub struct TopicController {
    topics: StoreContext<TopicSpec>,
    partitions: StoreContext<PartitionSpec>,
    spus: StoreContext<SpuSpec>,
    topic_epoch: Epoch,
    reducer: TopicReducer,
}

impl TopicController {
    /// streaming coordinator controller constructor
    pub fn start(ctx: SharedContext) {
        let topics = ctx.topics().clone();
        let partitions = ctx.partitions().clone();
        let topic_epoch = topics.store().init_epoch().epoch();

        let controller = Self {
            reducer: TopicReducer::new(
                topics.store().clone(),
                ctx.spus().store().clone(),
                partitions.store().clone(),
            ),
            topics,
            partitions,
            topic_epoch,
            spus: ctx.spus().clone(),
        };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        use tokio::select;

        self.sync_topics().await;

        loop {
            select! {
                _ = self.spus.listen() => {
                    debug!("detected events spu store");
                },
                _ = self.topics.listen() => {

                    self.sync_topics().await;
                }
            }
        }

        //debug!("spu controller is terminated");
    }

    /// get list of topics we need to check
    async fn sync_topics(&mut self) {
        let read_guard = self.topics.store().read().await;
        let (updates, _) = read_guard.changes_since(self.topic_epoch).parts();
        drop(read_guard);

        let actions = self.reducer.process_requests(updates).await;

        if actions.topics.is_empty() && actions.partitions.is_empty() {
            debug!("no actions needed");
        } else {
            debug!(
                "sending topic actions: {}, partition actions: {}",
                actions.topics.len(),
                actions.partitions.len()
            );
            for action in actions.topics.into_iter() {
                self.topics.send_action(action).await;
            }

            for action in actions.partitions.into_iter() {
                self.partitions.send_action(action).await;
            }
        }
    }
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
