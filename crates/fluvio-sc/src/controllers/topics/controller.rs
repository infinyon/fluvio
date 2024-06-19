//!
//! # Topic Controller
//!
//! Reconcile Topics

use std::cmp::min;
use std::ops::Add;

use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::topic::CleanupPolicy;
use fluvio_controlplane_metadata::topic::SegmentBasedPolicy;
use fluvio_controlplane_metadata::topic::TopicStorageConfig;
use fluvio_stream_dispatcher::actions::WSAction;
use fluvio_stream_model::core::MetadataItem;
use fluvio_stream_model::store::ChangeListener;
use fluvio_stream_model::store::k8::K8MetaItem;
use fluvio_types::defaults::{STORAGE_RETENTION_SECONDS, CONSUMER_STORAGE_TOPIC};
use tracing::{info, instrument, trace, debug};

use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::topic::TopicSpec;
use crate::stores::partition::PartitionSpec;
use crate::stores::StoreContext;

use super::actions::TopicActions;
use super::reducer::TopicReducer;

const OFFSET_TOPIC_SEGMENT_SIZE: u32 = 512_000_000; // 512MB
const OFFSET_TOPIC_PARTITION_SIZE: u64 = OFFSET_TOPIC_SEGMENT_SIZE as u64 * 4; // 2GB
const OFFSET_TOPIC_RETENTION_SEC: u32 = STORAGE_RETENTION_SECONDS; // 7 days

#[derive(Debug)]
pub struct TopicController<C: MetadataItem = K8MetaItem> {
    spus: StoreContext<SpuSpec, C>,
    topics: StoreContext<TopicSpec, C>,
    partitions: StoreContext<PartitionSpec, C>,
    reducer: TopicReducer<C>,
}

impl<C> TopicController<C>
where
    C: MetadataItem + 'static,
    C::UId: Send + Sync,
{
    /// streaming coordinator controller constructor
    pub fn start(ctx: SharedContext<C>) {
        let topics = ctx.topics().clone();
        let partitions = ctx.partitions().clone();
        let spus = ctx.spus().clone();

        let controller = Self {
            reducer: TopicReducer::new(
                topics.store().clone(),
                ctx.spus().store().clone(),
                partitions.store().clone(),
            ),
            topics,
            partitions,
            spus,
        };

        spawn(controller.dispatch_loop());
    }
}

impl<C: MetadataItem> TopicController<C> {
    #[instrument(name = "TopicController", skip(self))]
    async fn dispatch_loop(mut self) {
        use std::time::Duration;

        use tokio::select;
        use fluvio_future::timer::sleep;

        debug!("starting dispatch loop");

        let mut topics_listener = self.topics.change_listener();
        let mut spus_listener = self.spus.change_listener();

        loop {
            self.sync_topics(&mut topics_listener).await;
            self.sync_spus(&mut spus_listener).await;

            select! {

                // just in case
                _ = sleep(Duration::from_secs(60)) => {
                    debug!("timer expired");
                },
                _ = topics_listener.listen() => {
                    debug!("detected topic changes");
                }
                _ = spus_listener.listen() => {
                    debug!("detected spu changes");
                }
            }
        }
    }

    #[instrument(skip(self, listener))]
    async fn sync_topics(&mut self, listener: &mut ChangeListener<TopicSpec, C>) {
        if !listener.has_change() {
            debug!("no change");
            return;
        }

        let changes = listener.sync_changes().await;

        if changes.is_empty() {
            debug!("no topic changes");
            return;
        }

        let (updates, _) = changes.parts();

        let actions = self.reducer.process_requests(updates).await;

        self.handle_actions(actions).await;
    }

    #[instrument(skip(self, listener))]
    async fn sync_spus(&mut self, listener: &mut ChangeListener<SpuSpec, C>) {
        if !listener.has_change() {
            debug!("no change");
            return;
        }

        let changes = listener.sync_changes().await;

        if changes.is_empty() {
            debug!("no spu changes");
            return;
        }

        let (updates, _) = changes.parts();

        if !updates.iter().any(|update| update.status.is_online()) {
            debug!("no new online spu");
            return;
        };

        let actions = self.reducer.process_spu_update().await;

        self.handle_actions(actions).await;
    }

    async fn handle_actions(&mut self, actions: TopicActions<C>) {
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

#[derive(Debug)]
pub struct SystemTopicController<C: MetadataItem = K8MetaItem> {
    topics: StoreContext<TopicSpec, C>,
}

impl<C> SystemTopicController<C>
where
    C: MetadataItem + 'static,
    C::UId: Send + Sync,
{
    pub fn start(ctx: SharedContext<C>) {
        let topics = ctx.topics().clone();

        let controller = Self { topics };

        spawn(controller.dispatch_loop());
    }

    #[instrument(name = "SystemTopicController", skip(self))]
    async fn dispatch_loop(mut self) {
        use std::time::Duration;

        use fluvio_future::timer::sleep;

        debug!("starting system topic dispatch loop");

        const INTERVAL_STEP: u64 = 15;
        const MAX_INTERVAL: u64 = 120;

        let mut interval_secs = INTERVAL_STEP;

        loop {
            sleep(Duration::from_secs(interval_secs)).await;
            self.ensure_offsets_topic_exists().await;
            interval_secs = min(MAX_INTERVAL, interval_secs.add(INTERVAL_STEP));
        }
    }

    async fn ensure_offsets_topic_exists(&mut self) {
        if self
            .topics
            .store()
            .read()
            .await
            .values()
            .any(|value| value.key().eq(CONSUMER_STORAGE_TOPIC))
        {
            trace!(CONSUMER_STORAGE_TOPIC, "topic exists");
        } else {
            let mut spec = TopicSpec::new_computed(1, 1, None);
            spec.set_system(true);
            spec.set_cleanup_policy(CleanupPolicy::Segment(SegmentBasedPolicy {
                time_in_seconds: OFFSET_TOPIC_RETENTION_SEC,
            }));
            spec.set_storage(TopicStorageConfig {
                segment_size: Some(OFFSET_TOPIC_SEGMENT_SIZE),
                max_partition_size: Some(OFFSET_TOPIC_PARTITION_SIZE),
            });
            self.topics
                .send_action(WSAction::UpdateSpec((
                    CONSUMER_STORAGE_TOPIC.to_string(),
                    spec,
                )))
                .await;
            info!(CONSUMER_STORAGE_TOPIC, "topic created");
        }
    }
}
