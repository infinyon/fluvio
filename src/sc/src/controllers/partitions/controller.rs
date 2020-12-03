//!
//! # Auth Controller
//!

use tracing::debug;

use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::{StoreContext, Epoch};
use crate::stores::partition::PartitionSpec;
use crate::stores::spu::SpuSpec;
use crate::stores::K8ChangeListener;

use super::reducer::*;

/// Handles Partition election
#[derive(Debug)]
pub struct PartitionController {
    partitions: StoreContext<PartitionSpec>,
    partition_epoch: Epoch,
    spus: StoreContext<SpuSpec>,
    spu_epoch: Epoch,
    reducer: PartitionReducer,
}

impl PartitionController {
    pub fn start(ctx: SharedContext) {
        let partitions = ctx.partitions().clone();
        let partition_epoch = partitions.store().init_epoch().spec_epoch();
        let spus = ctx.spus().clone();
        let spu_epoch = spus.store().init_epoch().spec_epoch();

        let controller = Self {
            partitions,
            partition_epoch,
            spus,
            spu_epoch,
            reducer: PartitionReducer::new(
                ctx.partitions().store().clone(),
                ctx.spus().store().clone(),
            ),
        };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        use tokio::select;

        debug!("starting dispatch loop");

        let mut spu_status_listener = self.spus.change_listener();
        loop {
            self.sync_spu_changes(&mut spu_status_listener).await;

            debug!("waiting for events");
            select! {

                _ = spu_status_listener.listen() => {
                   debug!("detected spus status changed");
                }
            }
        }

        // info!("spu controller is terminated");
    }

    /// sync spu states to partition
    /// check to make sure
    async fn sync_spu_changes(&mut self, listener: &mut K8ChangeListener<SpuSpec>) {
        if !listener.has_change() {
            debug!("no change");
            return;
        }

        debug!("sync spu changes");
        let changes = listener.sync_status_changes().await;
        if changes.is_empty() {
            debug!("no spu changes");
            return;
        }

        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();
        debug!(
            "received spu epoch: {}, updates: {},deletes: {}",
            epoch,
            updates.len(),
            deletes.len()
        );

        let actions = self
            .reducer
            .update_election_from_spu_changes(updates.into_iter().collect())
            .await;

        debug!("there were election actions: {}", actions.len());
        for action in actions.into_iter() {
            self.partitions.send_action(action).await;
        }
    }
}
