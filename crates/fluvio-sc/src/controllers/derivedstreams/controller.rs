use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::derivedstream::{DerivedStreamValidationInput};
use fluvio_controlplane_metadata::store::{ChangeListener, MetadataStoreObject};
use fluvio_controlplane_metadata::store::k8::K8MetaItem;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_stream_dispatcher::actions::WSAction;
use tokio::select;
use tracing::{debug, error, info, trace};
use tracing::instrument;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;

use crate::stores::derivedstream::DerivedStreamSpec;
use crate::stores::{StoreContext};

#[derive(Default)]
pub struct DerivedStreamControllerStat {
    outer_loop: AtomicU64,
    inner_loop: AtomicU64,
}

impl DerivedStreamControllerStat {
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

pub struct DerivedStreamController<C = K8MetaItem>
where
    C: MetadataItem + Send + Sync,
{
    derivedstreams: StoreContext<DerivedStreamSpec, C>,
    topics: StoreContext<TopicSpec, C>,
    modules: StoreContext<SmartModuleSpec, C>,
    stat: Arc<DerivedStreamControllerStat>,
}

impl<C> DerivedStreamController<C>
where
    C: MetadataItem + Send + Sync + 'static,
{
    /// create new smart stream controller and return stat
    pub fn start(
        derivedstreams: StoreContext<DerivedStreamSpec, C>,
        topics: StoreContext<TopicSpec, C>,
        modules: StoreContext<SmartModuleSpec, C>,
    ) -> Arc<DerivedStreamControllerStat> {
        let stat = Arc::new(DerivedStreamControllerStat::default());
        let controller = Self {
            derivedstreams,
            topics,
            modules,
            stat: stat.clone(),
        };

        spawn(controller.dispatch_loop());

        stat
    }

    #[instrument(skip(self), name = "DerivedStreamController")]
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

        let mut ss_listener = self.derivedstreams.change_listener();
        debug!("wait for initial sync for derivedstreams");
        let derivedstreams = ss_listener.wait_for_initial_sync().await;

        debug!("performing initial full validation");
        self.validating_derivedstream(derivedstreams, true).await;

        loop {
            debug!(
                inner_loop = self.stat.get_inner_loop(),
                "waiting for changes"
            );

            select! {

                _ = ss_listener.listen() => {
                    debug!("detected derivedstream changes");
                    self.sync_derivedstreams_changes(&mut ss_listener).await;
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

    /// validate derivedstream and update state if necessary
    #[instrument(skip(self,derivedstreams),fields(len = derivedstreams.len()))]
    async fn validating_derivedstream(
        &self,
        derivedstreams: Vec<MetadataStoreObject<DerivedStreamSpec, C>>,
        force: bool,
    ) {
        let inputs = DerivedStreamValidationInput {
            derivedstreams: self.derivedstreams.store(),
            topics: self.topics.store(),
            modules: self.modules.store(),
        };

        /*
        because of issue with tracing with async, enable only if you want to debug
        let topic_len = inputs.topics.count().await;
        let module_len = inputs.modules.count().await;
        let derivedstream_len = inputs.derivedstreams.count().await;

        debug!( topic_len,
            module_len,
            derivedstream_len,
        "validation inputs");
        */

        let mut actions = vec![];

        for derivedstream in derivedstreams.into_iter() {
            let mut status = derivedstream.status;
            trace!("incoming {:#?}", status);
            let key = derivedstream.key;
            if let Some(next_resolution) = status
                .resolution
                .next(&derivedstream.spec, &inputs, force)
                .await
            {
                debug!(?next_resolution,%key,"updated status");
                status.resolution = next_resolution;
                actions.push(WSAction::UpdateStatus::<DerivedStreamSpec, C>((
                    key, status,
                )));
            }
        }

        debug!(updates = actions.len(), "update actions");

        for action in actions.into_iter() {
            self.derivedstreams.send_action(action).await;
        }
    }

    /// update derivedstream changes
    #[instrument(skip(self))]
    async fn sync_derivedstreams_changes(
        &self,
        listener: &mut ChangeListener<DerivedStreamSpec, C>,
    ) {
        if !listener.has_change() {
            debug!("no change");
            return;
        }

        let changes = listener.sync_changes().await;

        if changes.is_empty() {
            debug!("no derivedstream changes");
            return;
        }

        let (updates, _) = changes.parts();

        self.validating_derivedstream(updates, false).await;
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

        self.validating_derivedstream(self.derivedstreams.store().clone_values().await, true)
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

        self.validating_derivedstream(self.derivedstreams.store().clone_values().await, true)
            .await;
    }
}

#[cfg(test)]
mod test {

    use fluvio_stream_dispatcher::dispatcher::memory::MemoryDispatcher;
    use fluvio_stream_model::store::memory::MemoryMeta;

    use super::*;

    use fluvio_controlplane_metadata::{
        derivedstream::{DerivedStreamInputRef, DerivedStreamRef, DerivedStreamResolution},
        store::{
            MetadataStoreObject,
            actions::{LSUpdate},
        },
    };

    const WAIT_TIME: Duration = Duration::from_millis(10);

    #[fluvio_future::test(ignore)]
    async fn test_smart_stream_controller() {
        let derivedstreams: StoreContext<DerivedStreamSpec, MemoryMeta> = StoreContext::new();
        let topics: StoreContext<TopicSpec, MemoryMeta> = StoreContext::new();
        let modules: StoreContext<SmartModuleSpec, MemoryMeta> = StoreContext::new();

        MemoryDispatcher::start(derivedstreams.clone());

        let stat =
            DerivedStreamController::start(derivedstreams.clone(), topics.clone(), modules.clone());

        // wait for controller to catch up
        sleep(WAIT_TIME).await;

        // do initial sync
        derivedstreams.store().sync_all(vec![]).await;
        topics.store().sync_all(vec![]).await;
        modules.store().sync_all(vec![]).await;

        // wait for controller to catch up
        sleep(WAIT_TIME).await;

        // add new derivedstream
        let sm1 = DerivedStreamSpec {
            input: DerivedStreamInputRef::Topic(DerivedStreamRef::new("topic1".to_string())),
            ..Default::default()
        };

        debug!("applying derivedstream update");
        derivedstreams
            .store()
            .apply_changes(vec![LSUpdate::Mod(MetadataStoreObject::with_spec(
                "sm1", sm1,
            ))])
            .await;

        // wait until controller sync
        sleep(WAIT_TIME).await;

        let sm1 = derivedstreams.store().value("sm1").await.expect("sm1");
        assert!(matches!(
            sm1.status.resolution,
            DerivedStreamResolution::InvalidConfig(_)
        ));

        // should only have 2 loops
        assert_eq!(stat.get_inner_loop(), 2);

        // we add topics
        // assert!(matches!(sm1.status.resolution, DerivedStreamResolution::Init));

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
        let sm1 = derivedstreams.store().value("sm1").await.expect("sm1");
        assert!(matches!(
            sm1.status.resolution,
            DerivedStreamResolution::Provisioned
        ));

        debug!("finished test");
    }
}
