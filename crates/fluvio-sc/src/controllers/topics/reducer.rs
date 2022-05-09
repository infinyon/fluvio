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

use tracing::{debug, trace, info, error, instrument};

use crate::stores::topic::*;
use crate::stores::partition::*;
use crate::stores::spu::*;
use crate::controllers::partitions::PartitionWSAction;

use super::*;

/// Generates Partition Spec from Toic Spec based on replication and partition factor.
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
pub struct TopicReducer {
    topic_store: Arc<TopicAdminStore>,
    spu_store: Arc<SpuAdminStore>,
    partition_store: Arc<PartitionAdminStore>,
}

impl Default for TopicReducer {
    fn default() -> Self {
        Self {
            topic_store: TopicAdminStore::new_shared(),
            spu_store: SpuAdminStore::new_shared(),
            partition_store: PartitionAdminStore::new_shared(),
        }
    }
}

impl TopicReducer {
    pub fn new<A, B, C>(topic_store: A, spu_store: B, partition_store: C) -> Self
    where
        A: Into<Arc<TopicAdminStore>>,
        B: Into<Arc<SpuAdminStore>>,
        C: Into<Arc<PartitionAdminStore>>,
    {
        Self {
            topic_store: topic_store.into(),
            spu_store: spu_store.into(),
            partition_store: partition_store.into(),
        }
    }

    #[allow(unused)]
    fn topic_store(&self) -> &TopicAdminStore {
        &self.topic_store
    }

    fn spu_store(&self) -> &SpuAdminStore {
        &self.spu_store
    }

    fn partition_store(&self) -> &PartitionAdminStore {
        &self.partition_store
    }

    pub async fn process_requests(&self, topic_updates: Vec<TopicAdminMd>) -> TopicActions {
        trace!("processing requests: {:#?}", topic_updates);

        let mut actions = TopicActions::default();

        for topic in topic_updates {
            self.update_actions_next_state(&topic, &mut actions).await;
        }

        actions
    }

    ///
    /// Compute next state for topic
    /// if state is different, apply actions
    ///
    #[instrument(level = "trace", skip(self, topic, actions))]
    async fn update_actions_next_state(&self, topic: &TopicAdminMd, actions: &mut TopicActions) {
        use fluvio_controlplane_metadata::core::MetadataItem;

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
                actions
                    .topics
                    .push(TopicWSAction::UpdateStatus((topic.key_owned(), status)));

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
                    debug!("Deleting partition: {}", partition.key());
                    actions
                        .partitions
                        .push(PartitionWSAction::Delete(partition.key_owned()));
                }

                return;
            }

            return;
        }

        let next_state =
            TopicNextState::compute_next_state(topic, self.spu_store(), self.partition_store())
                .await;

        debug!("topic: {} next state: {}", topic.key(), next_state);
        let mut updated_topic = topic.clone();
        trace!("next state: {:#?}", next_state);

        // apply changes in partitions
        for partition_kv in next_state
            .apply_as_next_state(&mut updated_topic)
            .into_iter()
        {
            actions
                .partitions
                .push(PartitionWSAction::Apply(partition_kv));
        }

        // apply changes to topics
        if updated_topic.status.resolution != topic.status.resolution
            || updated_topic.status.reason != topic.status.reason
        {
            info!(
                "{} status change to {} from: {}",
                topic.key(),
                updated_topic.status,
                topic.status
            );
            actions.topics.push(TopicWSAction::UpdateStatus((
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

    // if topic are just created, it should transitioned to pending state if config are valid
    #[fluvio_future::test]
    async fn test_topic_reducer_init_to_pending() {
        let topic_reducer = TopicReducer::default();
        let topic_requests = vec![
            TopicAdminMd::with_spec("topic1", (1, 1).into()),
            TopicAdminMd::with_spec("topic2", (2, 2).into()),
        ];

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
