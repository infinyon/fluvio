//!
//! # Auth Controller
//!

use tracing::{debug, trace, instrument};

use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::{StoreContext};
use crate::stores::partition::PartitionSpec;
use crate::stores::spu::SpuSpec;
use crate::stores::K8ChangeListener;

use super::reducer::PartitionReducer;

/// Handles Partition election
#[derive(Debug)]
pub struct PartitionController {
    partitions: StoreContext<PartitionSpec>,
    spus: StoreContext<SpuSpec>,
    reducer: PartitionReducer,
}

impl PartitionController {
    pub fn start(ctx: SharedContext) {
        let partitions = ctx.partitions().clone();
        let spus = ctx.spus().clone();

        let controller = Self {
            partitions,
            spus,
            reducer: PartitionReducer::new(
                ctx.partitions().store().clone(),
                ctx.spus().store().clone(),
            ),
        };

        spawn(controller.dispatch_loop());
    }

    #[instrument(skip(self), name = "PartitionController")]
    async fn dispatch_loop(mut self) {
        use tokio::select;

        let mut spu_status_listener = self.spus.change_listener();
        let mut partition_listener = self.partitions.change_listener();

        loop {
            self.sync_spu_changes(&mut spu_status_listener).await;
            self.sync_partition_changes(&mut partition_listener).await;

            trace!("waiting for events");

            select! {

                _ = spu_status_listener.listen() => {
                    debug!("detected spus status changed");
                },
                _ = partition_listener.listen() => {
                    debug!("detected partition changes");
                }

            }
        }

        // info!("spu controller is terminated");
    }

    #[instrument(skip(self, listener))]
    async fn sync_partition_changes(&mut self, listener: &mut K8ChangeListener<PartitionSpec>) {
        if !listener.has_change() {
            trace!("no partitions change");
            return;
        }

        // we only care about delete timestamp changes which are in metadata only
        let changes = listener.sync_meta_changes().await;
        if changes.is_empty() {
            trace!("no partition metadata changes");
            return;
        }

        let (updates, _) = changes.parts();
        trace!(
            meta_changes = &*format!("{:#?}", updates),
            "metadata changes"
        );

        let actions = self.reducer.process_partition_update(updates).await;

        debug!("generated partition actions: {}", actions.len());
        for action in actions.into_iter() {
            self.partitions.send_action(action).await;
        }
    }

    /// sync spu states to partition
    /// check to make sure
    async fn sync_spu_changes(&mut self, listener: &mut K8ChangeListener<SpuSpec>) {
        if !listener.has_change() {
            trace!("no spu changes");
            return;
        }

        trace!("sync spu changes");
        let changes = listener.sync_status_changes().await;
        if changes.is_empty() {
            trace!("no spu changes");
            return;
        }

        let epoch = changes.epoch;
        let (updates, deletes) = changes.parts();
        debug!(
            epoch,
            updates = updates.len(),
            deletes = deletes.len(),
            "spu changes",
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


/* 
#[cfg(test)]
mod test {



    #[fluvio_future::test(ignore)]

    async fn test_election() {


    }    

}
*/