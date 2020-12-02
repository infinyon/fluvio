//!
//! # Topic Controller
//!
//! Reconcile Topics

use tracing::debug;
use tracing::instrument;

use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::topic::TopicSpec;
use crate::stores::spu::SpuSpec;
use crate::stores::partition::PartitionSpec;
use crate::stores::{ StoreContext, StoreChanges};
use crate::stores::event::ChangeListener;

use super::reducer::TopicReducer;

#[derive(Debug)]
pub struct TopicController {
    topics: StoreContext<TopicSpec>,
    partitions: StoreContext<PartitionSpec>,
    spus: StoreContext<SpuSpec>,
    reducer: TopicReducer,
}

impl TopicController {
    /// streaming coordinator controller constructor
    pub fn start(ctx: SharedContext) {
        let topics = ctx.topics().clone();
        let partitions = ctx.partitions().clone();

        let controller = Self {
            reducer: TopicReducer::new(
                topics.store().clone(),
                ctx.spus().store().clone(),
                partitions.store().clone(),
            ),
            topics,
            partitions,
            spus: ctx.spus().clone(),
        };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        use std::time::Duration;

        use tokio::select;
        use fluvio_future::timer::sleep;

        debug!("starting dispatch loop");

        let mut spec_listener = self.topics.spec_listen();
        let mut status_listener = self.topics.status_listen();

        loop {
            self.sync_topics(&mut spec_listener, &mut status_listener)
                .await;

            select! {

                // just in case
                _ = sleep(Duration::from_secs(60)) => {
                    debug!("timer expired");
                },
                _ = spec_listener.listen() => {
                    debug!("detected topic spec changes");

                },
                _ = status_listener.listen() => {
                    debug!("detected topic status changes");
                }
            }
        }
    }

    /// sync topics with partition
    async fn sync_topics(&mut self, spec: &mut ChangeListener, status: &mut ChangeListener) {
        if spec.has_change() {
            self.sync_changes(self.topics.store().spec_changes_since(spec).await).await;
        }

        if status.has_change() {
            self.sync_changes(self.topics.store().status_changes_since(status).await).await;
        }
    }
     
    #[instrument(skip(self))]
    async fn sync_changes(&mut self,changes: StoreChanges<TopicSpec>) {
       
        let (updates, _) = changes.parts();
        

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
