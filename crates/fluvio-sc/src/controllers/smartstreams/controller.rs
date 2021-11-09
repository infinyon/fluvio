use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::smartstream::{SmartStreamValidationInput};
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

#[derive(Default)]
pub struct SmartStreamControllerStat {
    outer_loop: AtomicU64,
    inner_loop: AtomicU64,
}

impl SmartStreamControllerStat {
    pub fn inc_outer_loop(&self) {
        self.outer_loop
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn inc_inner_loop(&self) {
        self.inner_loop
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    #[inline]
    pub fn get_outer_loop(&self) -> u64 {
        self.outer_loop.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[inline]
    pub fn get_inner_loop(&self) -> u64 {
        self.inner_loop.load(std::sync::atomic::Ordering::SeqCst)
    }
}

pub struct SmartStreamController<C = K8MetaItem>
where
    C: MetadataItem + Send + Sync,
{
    smartstreams: StoreContext<SmartStreamSpec, C>,
    topics: StoreContext<TopicSpec, C>,
    modules: StoreContext<SmartModuleSpec, C>,
    stat: Arc<SmartStreamControllerStat>,
}

impl<C> SmartStreamController<C>
where
    C: MetadataItem + Send + Sync + 'static,
{
    /// create new smart stream controller and return stat
    pub fn start(
        smartstreams: StoreContext<SmartStreamSpec, C>,
        topics: StoreContext<TopicSpec, C>,
        modules: StoreContext<SmartModuleSpec, C>,
    ) -> Arc<SmartStreamControllerStat> {
        let stat = Arc::new(SmartStreamControllerStat::default());
        let controller = Self {
            smartstreams,
            topics,
            modules,
            stat: stat.clone(),
        };

        spawn(controller.dispatch_loop());

        stat
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
            self.stat.inc_outer_loop();
        }
    }

    #[instrument(skip(self),fields(outer = self.stat.get_outer_loop()))]
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

        debug!("performing initial full validation");
        self.validating_smartstream(smartstreams, true).await;

        loop {
            debug!(
                inner_loop = self.stat.get_inner_loop(),
                "waiting for changes"
            );

            select! {

                _ = ss_listener.listen() => {
                    debug!("detected smartstream changes");
                    self.sync_smartstreams_changes(&mut ss_listener).await;
                },
                _ = module_listener.listen() => {
                    debug!("detected module changes");
                    self.sync_module_changes(&mut module_listener).await;
                },
                _ = topics_listener.listen() => {
                    debug!("detected topic changes");
                    self.sync_topic_changes(&mut topics_listener).await;
                },
            }

            self.stat.inc_inner_loop();
        }
    }

    /// validate smartstream and update state if necessary
    #[instrument(skip(self,smartstreams),fields(len = smartstreams.len()))]
    async fn validating_smartstream(
        &self,
        smartstreams: Vec<MetadataStoreObject<SmartStreamSpec, C>>,
        force: bool,
    ) {
        let inputs = SmartStreamValidationInput {
            smartstreams: self.smartstreams.store(),
            topics: self.topics.store(),
            modules: self.modules.store(),
        };

        /*
        because of issue with tracing with async, enable only if you want to debug
        let topic_len = inputs.topics.count().await;
        let module_len = inputs.modules.count().await;
        let smartstream_len = inputs.smartstreams.count().await;

        debug!( topic_len,
            module_len,
            smartstream_len,
        "validation inputs");
        */

        let mut actions = vec![];

        for smartstream in smartstreams.into_iter() {
            let mut status = smartstream.status;
            trace!("incoming {:#?}", status);
            let key = smartstream.key;
            if let Some(next_resolution) = status
                .resolution
                .next(&smartstream.spec, &inputs, force)
                .await
            {
                debug!(?next_resolution,%key,"updated status");
                status.resolution = next_resolution;
                actions.push(WSAction::UpdateStatus::<SmartStreamSpec, C>((key, status)));
            }
        }

        debug!(updates = actions.len(), "update actions");

        for action in actions.into_iter() {
            self.smartstreams.send_action(action).await;
        }
    }

    /// update smartstream changes
    #[instrument(skip(self))]
    async fn sync_smartstreams_changes(&self, listener: &mut ChangeListener<SmartStreamSpec, C>) {
        if !listener.has_change() {
            debug!("no change");
            return;
        }

        let changes = listener.sync_changes().await;

        if changes.is_empty() {
            debug!("no smartstream changes");
            return;
        }

        let (updates, _) = changes.parts();

        self.validating_smartstream(updates, false).await;
    }

    async fn sync_module_changes(&self, listener: &mut ChangeListener<SmartModuleSpec, C>) {
        if !listener.has_change() {
            debug!("no change");
            return;
        }

        let changes = listener.sync_changes().await;

        if changes.is_empty() {
            debug!("no modules changes");
            return;
        }

        // for now, we do full check regardless of partial changes.

        self.validating_smartstream(self.smartstreams.store().clone_values().await, true)
            .await;
    }

    async fn sync_topic_changes(&self, listener: &mut ChangeListener<TopicSpec, C>) {
        if !listener.has_change() {
            debug!("no change");
            return;
        }

        let changes = listener.sync_changes().await;

        if changes.is_empty() {
            debug!("no topic changes");
            return;
        }

        // for now, we do full check regardless of partial changes.

        self.validating_smartstream(self.smartstreams.store().clone_values().await, true)
            .await;
    }
}

#[cfg(test)]
mod test {

    use fluvio_stream_dispatcher::dispatcher::memory::MemoryDispatcher;
    use fluvio_stream_model::store::memory::MemoryMeta;

    use super::*;

    use fluvio_controlplane_metadata::{
        smartstream::{SmartStreamInput, SmartStreamRef, SmartStreamResolution},
        store::{
            MetadataStoreObject,
            actions::{LSUpdate},
        },
    };

    const WAIT_TIME: Duration = Duration::from_millis(10);

    #[fluvio_future::test(ignore)]
    async fn test_smart_stream_controller() {
        let smartstreams: StoreContext<SmartStreamSpec, MemoryMeta> = StoreContext::new();
        let topics: StoreContext<TopicSpec, MemoryMeta> = StoreContext::new();
        let modules: StoreContext<SmartModuleSpec, MemoryMeta> = StoreContext::new();

        MemoryDispatcher::start(smartstreams.clone());

        let stat =
            SmartStreamController::start(smartstreams.clone(), topics.clone(), modules.clone());

        // wait for controller to catch up
        sleep(WAIT_TIME).await;

        // do initial sync
        smartstreams.store().sync_all(vec![]).await;
        topics.store().sync_all(vec![]).await;
        modules.store().sync_all(vec![]).await;

        // wait for controller to catch up
        sleep(WAIT_TIME).await;

        // add new smartstream
        let sm1 = SmartStreamSpec {
            input: SmartStreamInput::Topic(SmartStreamRef::new("topic1".to_string())),
            ..Default::default()
        };

        debug!("applying smartstream update");
        smartstreams
            .store()
            .apply_changes(vec![LSUpdate::Mod(MetadataStoreObject::with_spec(
                "sm1", sm1,
            ))])
            .await;

        // wait until controller sync
        sleep(WAIT_TIME).await;

        let sm1 = smartstreams.store().value("sm1").await.expect("sm1");
        assert!(matches!(
            sm1.status.resolution,
            SmartStreamResolution::InvalidConfig(_)
        ));

        // should only have 2 loops
        assert_eq!(stat.get_inner_loop(), 2);

        // we add topics
        // assert!(matches!(sm1.status.resolution, SmartStreamResolution::Init));

        let topic1 = TopicSpec::default();

        debug!("creating new topic");
        topics
            .store()
            .apply_changes(vec![LSUpdate::Mod(MetadataStoreObject::with_spec(
                "topic1", topic1,
            ))])
            .await;

        sleep(WAIT_TIME).await;

        // this should in valid stat
        let sm1 = smartstreams.store().value("sm1").await.expect("sm1");
        assert!(matches!(
            sm1.status.resolution,
            SmartStreamResolution::Provisioned
        ));

        debug!("finished test");
    }
}
