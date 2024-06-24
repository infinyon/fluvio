//!
//! # Topic & Topics Metadata
//!
//! Topic metadata information cached on SC.
//!
//! # Remarks
//! Topic Status uses TopicResolution to reflect the state of the replica map:
//!     Ok,           // replica map has been generated, topic is operational
//!     Pending,      // not enough SPUs to generate "replica map"
//!     Inconsistent, // use change spec parameters, which is not supported
//!     InvalidConfig, // invalid configuration parameters provided
//!
use std::sync::Arc;

use fluvio_stream_dispatcher::actions::WSAction;
use fluvio_stream_model::core::MetadataItem;
use tracing::{debug, trace, error, instrument};

use crate::controllers::scheduler::PartitionScheduler;
use crate::controllers::topics::policy::TopicNextState;
use crate::stores::topic::*;
use crate::stores::partition::*;
use crate::stores::spu::*;
use crate::controllers::partitions::PartitionWSAction;

use super::actions::TopicActions;

/// Generates Partition Spec from Topic Spec based on replication and partition factor.
/// For example, if we have Topic with partitions = #1 and replication = #2,
/// it will generates Partition with name "Topic-0" with Replication of 2.
///
/// Generated Partition looks like below.  Initially, it is not assigned to any SPU
///      Spec
///           name: Topic-0
///           replication: 2
///      Status
///           state: Init
///
///
/// Actually replica assignment is done by Partition controller.
#[derive(Debug)]
pub struct TopicReducer<C: MetadataItem = K8MetaItem> {
    topic_store: Arc<TopicLocalStore<C>>,
    spu_store: Arc<SpuLocalStore<C>>,
    partition_store: Arc<PartitionLocalStore<C>>,
}

impl<C: MetadataItem> TopicReducer<C> {
    pub fn new(
        topic_store: impl Into<Arc<TopicLocalStore<C>>>,
        spu_store: impl Into<Arc<SpuLocalStore<C>>>,
        partition_store: impl Into<Arc<PartitionLocalStore<C>>>,
    ) -> Self {
        Self {
            topic_store: topic_store.into(),
            spu_store: spu_store.into(),
            partition_store: partition_store.into(),
        }
    }

    #[allow(unused)]
    fn topic_store(&self) -> &TopicLocalStore<C> {
        &self.topic_store
    }

    fn spu_store(&self) -> &SpuLocalStore<C> {
        &self.spu_store
    }

    fn partition_store(&self) -> &Arc<PartitionLocalStore<C>> {
        &self.partition_store
    }

    pub async fn process_requests(&self, topic_updates: Vec<TopicMetadata<C>>) -> TopicActions<C> {
        trace!(?topic_updates, "processing requests");

        let mut actions = TopicActions::default();

        for topic in topic_updates {
            self.update_actions_next_state(&topic, &mut actions).await;
        }

        actions
    }

    pub async fn process_spu_update(&self) -> TopicActions<C> {
        let mut actions = TopicActions::default();

        let topics = self.topic_store().read().await;
        for topic in topics.values() {
            self.update_actions_next_state(topic, &mut actions).await;
        }

        actions
    }

    ///
    /// Compute next state for topic
    /// if state is different, apply actions
    ///
    #[instrument(skip(self, actions))]
    async fn update_actions_next_state(
        &self,
        topic: &TopicMetadata<C>,
        actions: &mut TopicActions<C>,
    ) {
        // wait for partition store to be initially loaded
        self.partition_store().wait_for_first_change().await;

        // if foregroundDeletion is the finalizer, then we can mark it as delete
        if topic.ctx().item().is_being_deleted() {
            // set to delete if not it set
            if !topic.status.resolution().is_being_deleted() {
                debug!(
                    "topic has foreground delete but delete status is not set: {}",
                    topic.key()
                );
                let mut status = topic.status().clone();
                status.resolution = TopicResolution::Deleting;
                actions.topics.push(WSAction::<TopicSpec, C>::UpdateStatus((
                    topic.key_owned(),
                    status,
                )));

                // find children and delete them
                let partitions = topic.childrens(self.partition_store()).await;

                if partitions.is_empty() {
                    error!(
                        "no children found for topic: {} when trying to delete",
                        topic.key()
                    );
                    return;
                }
                for partition in partitions.into_iter() {
                    debug!(partition = %partition.key(), "Deleting partition");
                    actions
                        .partitions
                        .push(PartitionWSAction::Delete(partition.key_owned()));
                }

                return;
            }

            return;
        }

        if topic.status().is_resolution_provisioned()
            && topic.spec().replicas().partitions() > topic.status().replica_map.len() as u32
        {
            debug!(
                "topic: {} has not enough partitions, waiting for more",
                topic.key()
            );
            let mut status = topic.status().clone();
            status.resolution = TopicResolution::Pending;
            actions.topics.push(WSAction::<TopicSpec, C>::UpdateStatus((
                topic.key_owned(),
                status,
            )));
            return;
        }

        let mut scheduler =
            PartitionScheduler::init(self.spu_store(), self.partition_store()).await;
        let next_state = TopicNextState::compute_next_state(topic, &mut scheduler).await;

        debug!(topic = %topic.key(), ?next_state, "topic and next");
        let mut updated_topic = topic.clone();
        trace!(?next_state, "next state");

        // apply changes in partitions
        for partition_kv in next_state
            .apply_as_next_state(&mut updated_topic)
            .into_iter()
        {
            actions
                .partitions
                .push(WSAction::<PartitionSpec, C>::Apply(partition_kv));
        }

        // apply changes to topics
        if updated_topic.status.resolution != topic.status.resolution
            || updated_topic.status.reason != topic.status.reason
        {
            debug!(
                topic = %topic.key(),
                old_status = ?topic.status,
                new_status = ?updated_topic.status,
                "updating topic status"
            );
            actions.topics.push(WSAction::<TopicSpec, C>::UpdateStatus((
                updated_topic.key_owned(),
                updated_topic.status,
            )));
        }
    }
}

#[cfg(test)]
mod test2 {

    use fluvio_controlplane_metadata::topic::{TopicResolution, TopicStatus};
    use fluvio_controlplane_metadata::topic::PENDING_REASON;

    use super::*;

    type TopicWSAction = WSAction<TopicSpec, K8MetaItem>;

    // if topic are just created, it should transitioned to pending state if config are valid
    #[fluvio_future::test]
    async fn test_topic_reducer_init_to_pending() {
        let partition_store = PartitionLocalStore::new_shared();
        let topic_reducer = TopicReducer::new(
            TopicLocalStore::new_shared(),
            SpuLocalStore::new_shared(),
            partition_store.clone(),
        );
        let topic_requests = vec![
            TopicAdminMd::with_spec("topic1", (1, 1).into()),
            TopicAdminMd::with_spec("topic2", (2, 2).into()),
        ];

        partition_store.sync_all(vec![]).await;
        let actions = topic_reducer.process_requests(topic_requests).await;

        // topic key/value store actions
        let expected_actions: Vec<TopicWSAction> = vec![
            TopicWSAction::UpdateStatus((
                "topic1".into(),
                TopicStatus::new(TopicResolution::Pending, vec![], PENDING_REASON),
            )),
            TopicWSAction::UpdateStatus((
                "topic2".into(),
                TopicStatus::new(TopicResolution::Pending, vec![], PENDING_REASON),
            )),
        ];
        assert_eq!(actions.topics, expected_actions);
    }
}
