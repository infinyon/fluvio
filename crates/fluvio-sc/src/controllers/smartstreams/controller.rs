use std::time::Duration;

use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::smartstream::{SmartStreamResolution, SmartStreamValidationInput};
use fluvio_controlplane_metadata::store::{ChangeListener, MetadataStoreObject};
use fluvio_controlplane_metadata::store::k8::K8MetaItem;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_stream_dispatcher::actions::WSAction;
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
    topics: StoreContext<TopicSpec, C>,
    modules: StoreContext<SmartModuleSpec, C>,
    cycles: u64, // number of cycles
}

impl<C> SmartStreamController<C>
where
    C: MetadataItem + Send + Sync + 'static,
{
    pub fn start(
        smartstreams: StoreContext<SmartStreamSpec, C>,
        topics: StoreContext<TopicSpec, C>,
        modules: StoreContext<SmartModuleSpec, C>,
    ) {
        let controller = Self {
            smartstreams,
            topics,
            modules,
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

        let mut topics_listener = self.topics.change_listener();
        debug!("wait for initial sync for topics");
        let _ = topics_listener.wait_for_initial_sync().await;

        let mut module_listener = self.modules.change_listener();
        debug!("wait for initial sync for modules");
        let _ = module_listener.wait_for_initial_sync().await;

        let mut ss_listener = self.smartstreams.change_listener();
        debug!("wait for initial sync for smartstreams");
        let smartstreams = ss_listener.wait_for_initial_sync().await;

        self.full_sync_smartstream(smartstreams).await;

        loop {
            select! {

                _ = ss_listener.listen() => {
                    debug!("detected smartstream changes");


                }
            }
        }
    }

    /// update smartstream state assuming other objects are already synced
    #[instrument(skip(self))]
    async fn full_sync_smartstream(
        &mut self,
        smartstreams: Vec<MetadataStoreObject<SmartStreamSpec, C>>,
    ) {
        let inputs = SmartStreamValidationInput {
            smartstreams: self.smartstreams.store(),
            topics: self.topics.store(),
            modules: self.modules.store(),
        };

        let mut actions = vec![];
        for smartstream in smartstreams.into_iter() {
            let mut status = smartstream.status;
            let key = smartstream.key;
            if let Some(next_resolution) = status.resolution.next(&smartstream.spec, &inputs).await
            {
                status.resolution = next_resolution;
                actions.push(WSAction::UpdateStatus::<SmartStreamSpec, C>((key, status)));
            }
        }

        for action in actions.into_iter() {
            self.smartstreams.send_action(action).await;
        }
    }
}

#[cfg(test)]
mod test {

    use fluvio_stream_model::store::memory::MemoryMeta;

    use crate::controllers::smartstreams;

    use super::*;

    use fluvio_controlplane_metadata::{
        smartstream::{SmartStreamInput, SmartStreamInputs, SmartStreamRef},
        store::{
            MetadataStoreObject,
            actions::{LSChange, LSUpdate},
        },
    };

    type MemSmartStreams = MetadataStoreObject<SmartStreamSpec, MemoryMeta>;
    type MemSmartStream = MetadataStoreObject<SmartStreamSpec, MemoryMeta>;

    #[fluvio_future::test(ignore)]
    async fn test_smart_stream_controller() {
        let smartstreams: StoreContext<SmartStreamSpec, MemoryMeta> = StoreContext::new();
        let topics: StoreContext<TopicSpec, MemoryMeta> = StoreContext::new();
        let modules: StoreContext<SmartModuleSpec, MemoryMeta> = StoreContext::new();

        let _controller =
            SmartStreamController::start(smartstreams.clone(), topics.clone(), modules.clone());

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
