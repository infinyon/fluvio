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
        use std::time::Duration;

        use tokio::select;
        use fluvio_future::timer::sleep;

        debug!("starting topic controller loop");

        let mut timer = sleep(Duration::from_secs(60));

        loop {
            self.sync_topics().await;
            
            select! {
                // this is hack until we fix listener
                _ = &mut timer => {
                    debug!("timer expired");
                },
                _ = self.topics.spec_listen() => {
                    debug!("detected topic spec changes. topic syncing");

                },
                _ = self.topics.status_listen() => {
                    debug!("detected topic status changes, topic syncing");
                }
            }
        }
    }

    /// get list of topics we need to check
    async fn sync_topics(&mut self) {
        debug!("syncing topics");
        let read_guard = self.topics.store().read().await;
        let changes = read_guard.changes_since(self.topic_epoch);
        self.topic_epoch = changes.epoch;
        debug!("setting topic epoch to: {}", self.topic_epoch);
        let (updates, _) = changes.parts();
        debug!("updates: {}", updates.len());
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