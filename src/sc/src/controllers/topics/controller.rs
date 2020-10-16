//!
//! # Topic Controller
//!
//! Reconcile Topics

use tracing::debug;

use fluvio_future::task::spawn;

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
        let topic_epoch = topics.store().init_epoch().spec_epoch();

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

        debug!("starting topic controller loop");
        self.sync_topics().await;

        loop {
            debug!("waiting for events");
            select! {
                _ = self.spus.spec_listen() => {
                    debug!("detected changes in spu spec");
                },
                _ = self.topics.spec_listen() => {
                    debug!("detected topic spec changes. topic syncing");
                    self.sync_topics().await;
                }
            }
        }

        //debug!("spu controller is terminated");
    }

    /// get list of topics we need to check
    async fn sync_topics(&mut self) {
        let read_guard = self.topics.store().read().await;
        let (updates, _) = read_guard.spec_changes_since(self.topic_epoch).parts();
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
