//!
//! # Auth Controller
//!

use std::time::Duration;

use fluvio_controlplane_metadata::store::ChangeListener;
use fluvio_future::timer::sleep;
use tracing::{debug, trace, info, error, instrument};

use fluvio_future::task::spawn;
use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_controlplane_metadata::store::k8::K8MetaItem;

use crate::stores::{StoreContext};
use crate::stores::partition::PartitionSpec;
use crate::stores::spu::SpuSpec;

use super::reducer::PartitionReducer;

/// Handles Partition election
#[derive(Debug)]
pub struct PartitionController<C = K8MetaItem>
where
    C: MetadataItem + Send + Sync,
{
    partitions: StoreContext<PartitionSpec, C>,
    spus: StoreContext<SpuSpec, C>,
    reducer: PartitionReducer<C>,
}

impl<C> PartitionController<C>
where
    C: MetadataItem + Send + Sync + 'static,
{
    pub fn start(partitions: StoreContext<PartitionSpec, C>, spus: StoreContext<SpuSpec, C>) {
        let controller = Self {
            reducer: PartitionReducer::new(partitions.store().clone(), spus.store().clone()),
            partitions,
            spus,
        };

        spawn(controller.dispatch_loop());
    }
}

impl<C> PartitionController<C>
where
    C: MetadataItem + Send + Sync,
{
    #[instrument(skip(self), name = "PartitionController")]
    async fn dispatch_loop(mut self) {
        info!("started");
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with inner loop: {:#?}", err);
                debug!("sleeping 10 seconds try again");
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    async fn inner_loop(&mut self) -> Result<(), ()> {
        use tokio::select;

        debug!("initializing listeners");
        let mut spu_status_listener = self.spus.change_listener();
        let _ = spu_status_listener.wait_for_initial_sync().await;

        let mut partition_listener = self.partitions.change_listener();
        let _ = partition_listener.wait_for_initial_sync().await;

        debug!("finish initializing listeners");

        loop {
            self.sync_spu_changes(&mut spu_status_listener).await;
            self.sync_partition_changes(&mut partition_listener).await;

            trace!("waiting for events");

            select! {

                _ = spu_status_listener.listen() => {
                    debug!("detected spus changes");
                },
                _ = partition_listener.listen() => {
                    debug!("detected partition changes");
                }

            }
        }

        // info!("spu controller is terminated");
    }

    #[instrument(skip(self, listener))]
    async fn sync_partition_changes(&mut self, listener: &mut ChangeListener<PartitionSpec, C>) {
        if !listener.has_change() {
            trace!("no partitions change");
            return;
        }

        // we only care about delete timestamp changes which are in metadata only
        let changes = listener.sync_meta_changes().await;
        if changes.is_empty() {
            debug!("no partition metadata changes");
            return;
        }

        let (updates, _) = changes.parts();
        trace!(meta_changes = &*format!("{updates:#?}"), "metadata changes");

        let actions = self.reducer.process_partition_update(updates).await;

        debug!("generated partition actions: {}", actions.len());
        for action in actions.into_iter() {
            self.partitions.send_action(action).await;
        }
    }

    /// sync spu states to partition
    /// check to make sure
    async fn sync_spu_changes(&mut self, listener: &mut ChangeListener<SpuSpec, C>) {
        if !listener.has_change() {
            trace!("no spu changes");
            return;
        }

        let changes = listener.sync_status_changes().await;
        if changes.is_empty() {
            debug!("no spu status changes");
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
TODO: in progress, do not delete
#[cfg(test)]
mod test {

    use super::*;

    mod meta {
        use fluvio_controlplane_metadata::core::{MetadataItem, MetadataRevExtension};

        /// simple memory representation of meta
        #[derive(Debug, Default, PartialEq, Clone)]
        pub struct MemoryMeta {
            pub rev: u32,
        }

        impl MetadataItem for MemoryMeta {
            type UId = u32;

            fn uid(&self) -> &Self::UId {
                &self.rev
            }

            fn is_newer(&self, another: &Self) -> bool {
                self.rev >= another.rev
            }
        }

        impl MetadataRevExtension for MemoryMeta {
            fn next_rev(&self) -> Self {
                Self { rev: self.rev + 1 }
            }
        }

        impl MemoryMeta {
            pub fn new(rev: u32) -> Self {
                Self { rev }
            }
        }

        /*
        impl From<u32> for MetadataContext<MemoryMeta> {
            fn from(val: u32) -> MetadataContext<MemoryMeta> {
                MemoryMeta::new(val).into()
            }
        }
        */
    }

    use fluvio_controlplane_metadata::store::MetadataStoreObject;
    use meta::*;

    type MemPartition = MetadataStoreObject<PartitionSpec, MemoryMeta>;
    type MemSpu = MetadataStoreObject<SpuSpec, MemoryMeta>;

    #[fluvio_future::test(ignore)]
    async fn test_election() {
        let partitions: StoreContext<PartitionSpec, MemoryMeta> = StoreContext::new();
        let spus: StoreContext<SpuSpec, MemoryMeta> = StoreContext::new();

        let controller = PartitionController::start(partitions.clone(), spus.clone());

        // add partitions
        spus.store().sync_all(vec![]).await;
    }
}
*/
