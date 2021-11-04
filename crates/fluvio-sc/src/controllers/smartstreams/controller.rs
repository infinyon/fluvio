use std::time::Duration;

use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_controlplane_metadata::store::ChangeListener;
use fluvio_controlplane_metadata::store::k8::K8MetaItem;
use tokio::select;
use tracing::{debug, error, info, trace};
use tracing::instrument;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;

use crate::stores::smartstream::SmartStreamSpec;
use crate::stores::{StoreContext};

pub struct SmartStreamController<C = K8MetaItem>
where
    C: MetadataItem + Send + Sync,
{
    smartstreams: StoreContext<SmartStreamSpec, C>,
    cycles: u64, // number of cycles
}

impl<C> SmartStreamController<C>
where
    C: MetadataItem + Send + Sync + 'static,
{
    pub fn start(smartstreams: StoreContext<SmartStreamSpec, C>) {
        let controller = Self {
            smartstreams,
            cycles: 0,
        };

        spawn(controller.dispatch_loop());
    }

    #[instrument(skip(self), name = "SmartStreamController")]
    async fn dispatch_loop(mut self) {
        info!("started");
        loop {
            if let Err(err) = self.inner_loop().await {
                // in case of error, log and sleep for a while
                error!("error with inner loop: {:#?}", err);
                debug!("sleeping 10 seconds try again");
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    #[instrument(skip(self),fields(cycles = self.cycles))]
    async fn inner_loop(&mut self) -> Result<(), ()> {
        debug!("starting inner loop");

        let mut listener = self.smartstreams.change_listener();

        debug!("wait for initial sync");
        let _ = listener.wait_for_initial_sync().await;

        loop {
            self.sync_smartstream(&mut listener).await;

            select! {

                _ = listener.listen() => {
                    debug!("detected smartstream changes");

                }
            }
        }
    }

    #[instrument(skip(self, listener))]
    async fn sync_smartstream(&mut self, listener: &mut ChangeListener<SmartStreamSpec, C>) {
        if !listener.has_change() {
            trace!("no spu changes");
            return;
        }

        let changes = listener.sync_changes().await;
        if changes.is_empty() {
            trace!("no smartstream changes");
            return;
        }

        debug!("detected smartstream changes");
        let (updates, _) = changes.parts();

        for update in updates.into_iter() {

            let spec = update.spec();
            let id = spec.id();
            let name = spec.name();
            let namespace = spec.namespace();

            info!("smartstream {}/{}/{} updated", namespace, name, id);
        

        }
        
    }
}

#[cfg(test)]
mod test {

    use crate::controllers::smartstreams;

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

    use fluvio_controlplane_metadata::{
        smartstream::{SmartStreamInput, SmartStreamInputs, SmartStreamRef},
        store::{
            MetadataStoreObject,
            actions::{LSChange, LSUpdate},
        },
    };
    use meta::*;

    type MemSmartStreams = MetadataStoreObject<SmartStreamSpec, MemoryMeta>;
    type MemSmartStream = MetadataStoreObject<SmartStreamSpec, MemoryMeta>;

    #[fluvio_future::test(ignore)]
    async fn test_smart_stream_controller() {
        let smartstreams: StoreContext<SmartStreamSpec, MemoryMeta> = StoreContext::new();

        let _controller = SmartStreamController::start(smartstreams.clone());

        // do initial sync
        smartstreams.store().sync_all(vec![]).await;

        // wait and apply
        sleep(Duration::from_secs(1)).await;

        // add new smartstream
        let sm1 = SmartStreamSpec {
            inputs: SmartStreamInputs {
                left: SmartStreamInput::Topic(SmartStreamRef::new("topic1".to_string())),
                ..Default::default()
            },
            ..Default::default()
        };
        smartstreams
            .store()
            .apply_changes(vec![LSUpdate::Mod(MetadataStoreObject::with_spec(
                "sm1", sm1,
            ))])
            .await;

        //  smartstreams.store().
        sleep(Duration::from_secs(10)).await;
    }
}
