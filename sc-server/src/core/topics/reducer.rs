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
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::{debug, trace};
use types::log_on_err;
use metadata::spu::SpuSpec;

use crate::core::spus::SpuLocalStore;
use crate::core::partitions::PartitionWSAction;
use crate::core::partitions::PartitionLocalStore;
use crate::core::common::LSChange;
use crate::ScServerError;

use super::TopicActions;
use super::TopicChangeRequest;
use super::TopicKV;
use super::TopicLocalStore;
use super::TopicWSAction;

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
    topic_store: Arc<TopicLocalStore>,
    spu_store: Arc<SpuLocalStore>,
    partition_store: Arc<PartitionLocalStore>,
}

impl Default for TopicReducer {
    fn default() -> Self {
        Self {
            topic_store: TopicLocalStore::new_shared(),
            spu_store: SpuLocalStore::new_shared(),
            partition_store: PartitionLocalStore::new_shared(),
        }
    }
}

impl TopicReducer {
    pub fn new<A, B, C>(topic_store: A, spu_store: B, partition_store: C) -> Self
    where
        A: Into<Arc<TopicLocalStore>>,
        B: Into<Arc<SpuLocalStore>>,
        C: Into<Arc<PartitionLocalStore>>,
    {
        Self {
            topic_store: topic_store.into(),
            spu_store: spu_store.into(),
            partition_store: partition_store.into(),
        }
    }
    fn topic_store(&self) -> &TopicLocalStore {
        &self.topic_store
    }

    fn spu_store(&self) -> &SpuLocalStore {
        &self.spu_store
    }

    fn partition_store(&self) -> &PartitionLocalStore {
        &self.partition_store
    }

    pub fn process_requests(
        &self,
        requests: TopicChangeRequest,
    ) -> Result<TopicActions, ScServerError> {
        
        trace!("processing requests: {}", requests);

        let mut actions = TopicActions::default();

        match requests {
            TopicChangeRequest::Topic(topic_requests) => {
                for topic_request in topic_requests.into_iter() {
                    match topic_request {
                        LSChange::Add(topic) => self.add_topic_action_handler(topic, &mut actions),

                        LSChange::Mod(new_topic, local_topic) => {
                            log_on_err!(self.mod_topic_action_handler(
                                new_topic,
                                local_topic,
                                &mut actions,
                            ));
                        }

                        LSChange::Delete(_) => {
                            // ignore for now
                        }
                    }
                }
            }

            TopicChangeRequest::Spu(spu_requests) => {
                for request in spu_requests.into_iter() {
                    self.process_spu_kv(request, &mut actions);
                }
            }
        }

        trace!("\n{}", self.topic_store().table_fmt());

        Ok(actions)
    }

    // -----------------------------------
    // Action Handlers
    // -----------------------------------

    ///
    /// Triggers new topic metadata is received.
    ///
    /// At this point, we only need to ensure that topic with init status can be moved
    /// to pending or error state
    ///
    fn add_topic_action_handler(&self, topic: TopicKV, actions: &mut TopicActions) {
        let name = topic.key();

        debug!("AddTopic({}) - {}", name, topic);

        self.update_actions_next_state(&topic, actions);
    }

    ///
    /// Mod Topic Action handler
    ///
    /// # Remarks
    /// Action handler performs the following operations:
    /// * compare new topic with local topic
    /// * update local cache (if chaged)
    /// * generate replica_map (if possible) and push partitions to KV store
    ///
    fn mod_topic_action_handler(
        &self,
        new_topic: TopicKV,
        old_topic: TopicKV,
        actions: &mut TopicActions,
    ) -> Result<(), IoError> {
        let name = new_topic.key();
        debug!("Handling ModTopic: {} ", name);

        // if spec changed - not a supported feature
        if new_topic.spec != old_topic.spec {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "topic '{}' - update spec... not implemented",
                    name)
            ));
        }

        // if topic changed, update status & notify partitions
        if new_topic.
        status != old_topic.status {
            self.update_actions_next_state(&new_topic, actions);
        }

        Ok(())
    }

    /// process kv
    fn process_spu_kv(&self, request: LSChange<SpuSpec>, actions: &mut TopicActions) {
        match request {
            LSChange::Add(new_spu) => {
                debug!("processing SPU add: {}", new_spu);
                
                self.generate_replica_map_for_all_topics_handler(actions);
                
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
    fn update_actions_next_state(&self, topic: &TopicKV, actions: &mut TopicActions) {
        
        let next_state = topic.compute_next_state(self.spu_store(), self.partition_store());
        
        debug!("topic: {} next state: {}",topic.key(),next_state);
        let mut updated_topic = topic.clone();
        trace!("next state detal: {:#?}", next_state);

        // apply changes in partitions
        for partition_kv in updated_topic.apply_next_state(next_state).into_iter() {
            actions
                .partitions
                .push(PartitionWSAction::Add(partition_kv));
        }

        // apply changes to topics
        if updated_topic.status.resolution != topic.status.resolution || updated_topic.status.reason != topic.status.reason {
            debug!("{} status change to {} from: {}",topic.key(),updated_topic.status,topic.status);
            actions.topics.push(TopicWSAction::UpdateStatus(updated_topic));
        } 

    }

    ///
    /// Generate Replica Map for all Topics handler
    ///
    /// # Remarks
    /// Generally triggered when a new live borker joins the cluster.
    /// The handler performs the following operations:
    /// * loop through all topics and identify the ones waiting for replica map
    /// * generate replica map
    /// * push push result to KV topic queue (if replica map generations succeeded)
    ///
    fn generate_replica_map_for_all_topics_handler(&self, actions: &mut TopicActions) {
        debug!("updating replica maps for topics are in pending state");

        // loop through topics & generate replica map (if needed)
        self.topic_store().visit_values(|topic| {
            if topic.status.need_replica_map_recal() {
                let name = topic.key();
                debug!("Generate R-MAP for: {:?}", name);
                // topic status to collect modifications
                self.update_actions_next_state(topic, actions);
            }
        });
    }
}

#[cfg(test)]
mod test2 {
    use metadata::topic::{TopicResolution, TopicStatus};
    use metadata::topic::PENDING_REASON;
    use utils::actions::Actions;

    use super::TopicReducer;
    use super::TopicChangeRequest;
    use super::TopicWSAction;
    use super::TopicKV;
    use super::super::TopicLSChange;

    // if topic are just created, it should transitioned to pending state if config are valid
    #[test]
    fn test_topic_reducer_init_to_pending() {
        let topic_reducer = TopicReducer::default();
        let topic_requests: Actions<TopicLSChange> = vec![
            TopicLSChange::add(TopicKV::with_spec("topic1", (1, 1).into())),
            TopicLSChange::add(TopicKV::with_spec("topic2", (2, 2).into())),
        ]
        .into();

        let actions = topic_reducer
            .process_requests(TopicChangeRequest::Topic(topic_requests))
            .expect("actions");

        // topic key/value store actions
        let expected_actions: Actions<TopicWSAction> = vec![
            TopicWSAction::UpdateStatus(TopicKV::new(
                "topic1",
                (1, 1).into(),
                TopicStatus::new(TopicResolution::Pending, vec![], PENDING_REASON),
            )),
            TopicWSAction::UpdateStatus(TopicKV::new(
                "topic2",
                (2, 2).into(),
                TopicStatus::new(TopicResolution::Pending, vec![], PENDING_REASON),
            )),
        ]
        .into();
        assert_eq!(actions.topics, expected_actions);
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
