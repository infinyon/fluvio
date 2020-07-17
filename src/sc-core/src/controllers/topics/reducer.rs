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

use log::{debug, trace};
use flv_metadata::spu::SpuSpec;

use flv_metadata::store::actions::*;
use crate::stores::topic::*;
use crate::stores::partition::*;
use crate::stores::spu::*;
use crate::controllers::partitions::PartitionWSAction;
use crate::stores::K8MetaItem;


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

        for mut topic in topic_updates {
            self.update_actions_next_state(&mut topic, &mut actions)
                .await;
        }

        actions
    }

    /// process kv
    async fn process_spu_kv(
        &self,
        request: LSChange<SpuSpec, K8MetaItem>,
        actions: &mut TopicActions,
    ) {
        match request {
            LSChange::Add(new_spu) => {
                debug!("processing SPU add: {}", new_spu.key());

                self.generate_replica_map_for_all_topics_handler(actions)
                    .await;
            }

            LSChange::Mod(_new_spu, _old_spu) => {
                //debug!("processing SPU mod: {}", new_spu);

                /*
                if new_spu.status != old_spu.status {
                    debug!("detected change in spu status");
                    // if spu comes online
                    //  * send SPU a full update
                    //  * ask topic to generate replica map for pending topics

                    if old_spu.status.is_offline() && new_spu.status.is_online() {
                        debug!("spu offline -> online");
                        self.generate_replica_map_for_all_topics_handler(actions)
                    }
                }
                */
            }
            _ => {
                // do nothing, ignore for now
            }
        }
    }

    ///
    /// Compute next state for topic
    /// if state is different, apply actions
    ///
    async fn update_actions_next_state(&self, topic: &TopicAdminMd, actions: &mut TopicActions) {
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
            debug!(
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

    ///
    /// check if any topics need to be updated. this can happens when
    ///  SPU states changed
    ///
    async fn generate_replica_map_for_all_topics_handler(&self, actions: &mut TopicActions) {
        let mut count = 0;
        // loop through topics & generate replica map (if needed)
        for topic in self.topic_store().read().await.values() {
            if topic.status.need_replica_map_recal() {
                let name = topic.key();
                debug!("updating topic state {:?}", name);
                // topic status to collect modifications
                self.update_actions_next_state(topic, actions).await;
                count += 1;
            }
        }

        debug!("topics {} updated", count);
    }
}

#[cfg(test)]
mod test2 {
    use flv_future_aio::test_async;
    use flv_metadata::topic::{TopicResolution, TopicStatus};
    use flv_metadata::topic::PENDING_REASON;

    use super::*;

    // if topic are just created, it should transitioned to pending state if config are valid
    #[test_async]
    async fn test_topic_reducer_init_to_pending() -> Result<(), ()> {
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
        ]
        .into();
        assert_eq!(actions.topics, expected_actions);
        Ok(())
    }

    /*
    #[test]
    fn test_process_topics_actions_with_topics() {

        let topic_reducer = TopicReducer::default();
        let topic_store = topic_reducer.topic_store();
        let spu_store = topic_reducer.spu_store();

        // initialize topics
        let topic = TopicKV::new(
            (1, 1, false).into(),
            TopicStatus::new(
                TopicResolution::Pending,
                vec![],
                "waiting for live spus".to_owned(),
            ),
        );

        topic_store.insert("topic1", topic.clone());
        topic_store.insert("topic2", topic.clone());


        spu_store.bulk_add(vec![
            // spu_id, online, rack
            (5000, true, None),
        ]);


        let mut spu_requests: Actions<SpuLSChange> = Actions::default();
        // add LSChange to turn on SPU
        spu_requests.push_once(
            SpuLSChange::Mod(
                "spu-5000".to_owned(),
                (5000,false,None).into(),
                (5000,true,None).into()));


        // Run Test
        let actions = topic_reducer.process_requests(TopicChangeRequest::Spu(spu_requests)).expect("requests");

        // compare store
        let expected_topics = TopicLocalStore::new_shared();
        expected_topics.insert("topic1", topic.clone());
        expected_topics.insert("topic2", topic.clone());
        assert_eq!(*topic_store,expected_topics);

        // partition actions
        let expected_actions_for_partition: Actions<PartitionWSAction>  = Actions::default();
        assert_eq!(actions.partitions, expected_actions_for_partition);

        // topic key/value store actions
        let exepected_topic = TopicKV::new(
            (1, 1, false).into(),
            TopicStatus::new(TopicResolution::Ok, vec![vec![5000]], "".to_owned()),
        );
        let mut expected_actions_for_kvs: Actions<TopicWSAction> = Actions::default();
        expected_actions_for_kvs.push(TopicWSAction::UpdateStatus(
            exepected_topic.clone(),
        ));
        expected_actions_for_kvs.push(TopicWSAction::UpdateStatus(
            exepected_topic,
        ));
        assert_eq!(actions.topics, expected_actions_for_kvs);
    }
    */
}
