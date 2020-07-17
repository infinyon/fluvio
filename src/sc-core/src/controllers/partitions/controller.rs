//!
//! # Auth Controller
//!

use log::debug;

use flv_future_aio::task::spawn;

use crate::core::SharedContext;
use crate::stores::*;
use crate::stores::partition::*;
use crate::stores::spu::*;


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
        let partition_epoch = partitions.store().init_epoch().epoch();
        let spus = ctx.spus().clone();
        let spu_epoch = spus.store().init_epoch().epoch();

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

        self.sync_spu_changes().await;

        loop {
            select! {

                _ = self.spus.listen() => {
                    self.sync_spu_changes().await;
                }
            }
        }

        // info!("spu controller is terminated");
    }

    /// sync spu states to partition
    /// check to make sure
    async fn sync_spu_changes(&mut self) {
        let read_guard = self.spus.store().read().await;
        let changes = read_guard.changes_since(self.partition_epoch);
        drop(read_guard);
        self.spu_epoch = changes.epoch;
        let (updates, deletes) = changes.parts();
        debug!(
            "received spu epoch: {}, updates: {},deletes: {}",
            self.spu_epoch,
            updates.len(),
            deletes.len()
        );

        let actions = self.reducer.update_election_from_spu_changes(updates).await;

        debug!("there were election actions: {}", actions.len());
        for action in actions.into_iter() {
            self.partitions.send_action(action).await;
        }
    }

    
}
