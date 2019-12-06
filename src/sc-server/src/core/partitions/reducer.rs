//!
//! # Partition & Partitions Metadata
//!
//! Partition metadata information on cached in the local Controller.
//!
use log::trace;
use log::debug;
use log::error;
use log::warn;

use types::log_on_err;
use metadata::partition::PartitionSpec;
use metadata::partition::PartitionResolution;
use metadata::partition::PartitionStatus;
use metadata::partition::ReplicaStatus;
use metadata::partition::ElectionPolicy;
use metadata::partition::ElectionScoring;
use internal_api::UpdateLrsRequest;

use crate::conn_manager::ConnectionRequest;
use crate::conn_manager::PartitionSpecChange;
use crate::core::common::LSChange;
use crate::core::common::WSAction;
use crate::core::spus::SharedSpuLocalStore;
use crate::core::spus::SpuLocalStore;
use crate::core::spus::SpuKV;
use crate::ScServerError;

use super::PartitionChangeRequest;
use super::PartitionActions;
use super::PartitionLocalStore;
use super::PartitionKV;
use super::SharedPartitionStore;

type PartitionWSAction = WSAction<PartitionSpec>;

/// Given This is a generated partition from TopicController, It will try to allocate assign replicas
/// to live SPU.
/// ```ignore
///     Spec
///           name: Topic0-0
///           replication: 2
///     Status
///           state: Init
///
/// Assuming there are 3 SPU's [0,1,2].  It will try allocate SPU and assign leader.
/// Rules are:
///       SPU id must be unique.  
///       SPU leader must be evently distributed from other Partition.
///
/// So after assignment, then Partition will look liks this
///     Spec
///         name: Topic0-0
///         replication: 2
///     Status
///         status: ready
///         spu:  [0,1]
///         leader: 0
///  
/// The SPU 0 then may be have replica map
///
///     Spec
///        id:  0
///     
///      Status
///         replicas: [Topic0-0]
///
///```
/// If there are another topic1 with same number of partiition and replica then, they will
/// have different leader because Topic0-0 already is using spu 0.
#[derive(Debug)]
pub struct PartitionReducer {
    partition_store: SharedPartitionStore,
    spu_store: SharedSpuLocalStore,
}

impl Default for PartitionReducer {
    fn default() -> Self {
        Self {
            partition_store: PartitionLocalStore::new_shared(),
            spu_store: SpuLocalStore::new_shared(),
        }
    }
}

impl PartitionReducer {
    pub fn new<A, B>(partition_store: A, spu_store: B) -> Self
    where
        A: Into<SharedPartitionStore>,
        B: Into<SharedSpuLocalStore>,
    {
        Self {
            partition_store: partition_store.into(),
            spu_store: spu_store.into(),
        }
    }

    ///
    /// Process Partition Actions - dispatch to ADD/MOD/DEL handlers
    ///
    pub fn process_requests(
        &self,
        requests: PartitionChangeRequest,
    ) -> Result<PartitionActions, ScServerError> {
        trace!("Processing requests: {}", requests);
        let mut actions = PartitionActions::default();

        match requests {
            PartitionChangeRequest::Partition(partition_requests) => {
                for partiton_request in partition_requests.into_iter() {
                    match partiton_request {
                        LSChange::Add(partition) => {
                            log_on_err!(self.add_partition_action_handler(partition, &mut actions));
                        }

                        LSChange::Mod(new_partition, old_partition) => {
                            log_on_err!(self.mod_partition_action_handler(
                                new_partition,
                                old_partition,
                                &mut actions,
                            ));
                        }

                        LSChange::Delete(partition) => {
                            log_on_err!(self.del_partition_action_handler(partition, &mut actions));
                        }
                    }
                }
            }
            PartitionChangeRequest::Spu(spu_requests) => {
                for spu_request in spu_requests.into_iter() {
                    debug!("SPU LS: {}", spu_request);
                    trace!("SPU LS: {:#?}", spu_request);
                    match spu_request {
                        LSChange::Mod(new_spu, old_spu) => {
                            if old_spu.status.is_online() && new_spu.status.is_offline() {
                                self.force_election_spu_off(new_spu, &mut actions);
                            } else {
                                if old_spu.status.is_offline() && new_spu.status.is_online() {
                                    self.force_election_spu_on(new_spu, &mut actions);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            PartitionChangeRequest::LrsUpdate(lrs_status) => {
                self.process_lrs_update_from_spu(lrs_status, &mut actions);
            }
        }

        Ok(actions)
    }

    fn add_partition_action_handler(
        &self,
        mut partition: PartitionKV,
        actions: &mut PartitionActions,
    ) -> Result<(), ScServerError> {
        debug!(
            "Handling Add Partition: {}, sending to SPU",
            partition.key()
        );
        trace!("Add Partition: {:#?}", partition);

        actions
            .conns
            .push(ConnectionRequest::Partition(PartitionSpecChange::Add(
                partition.key.clone(),
                partition.spec.clone(),
            )));
        // we set status to offline for all new partition until we know their status
        partition.status.resolution = PartitionResolution::Offline;
        debug!(
            "Partition: {} set to offline because it is new",
            partition.key()
        );
        actions
            .partitions
            .push(PartitionWSAction::UpdateStatus(partition));
        Ok(())
    }

    ///
    /// Modify Partition Action handler
    ///
    /// # Remarks
    /// Action handler performs the following operations:
    /// * update partition on local cluster cache
    /// * generate message for replica SPUs
    ///
    fn mod_partition_action_handler(
        &self,
        new_partition: PartitionKV,
        old_partition: PartitionKV,
        actions: &mut PartitionActions,
    ) -> Result<(), ScServerError> {
        trace!("mod partition {:#?}", new_partition);

        // send out to SPU only if spec changes
        if new_partition.spec != old_partition.spec {
            debug!(
                "Partition: {} spec changed, updating SPU",
                new_partition.key()
            );
            actions
                .conns
                .push(ConnectionRequest::Partition(PartitionSpecChange::Mod(
                    new_partition.key,
                    new_partition.spec,
                    old_partition.spec,
                )));
        } else {
            debug!(
                "Parttion: {} status change only, doing nothing",
                new_partition.key()
            );
        }

        Ok(())
    }

    ///
    /// Delete Partition Action handler
    ///
    /// # Remarks
    /// Action handler performs the following operations:
    /// * remove partition from local cluster cache
    /// * generate message for live replicas
    ///
    fn del_partition_action_handler(
        &self,
        partition: PartitionKV,
        _actions: &mut PartitionActions,
    ) -> Result<(), ScServerError> {
        debug!("DelPartition({}) - remove from metadata", partition.key());
        trace!("delete partition {:#?}", partition);

        /*
        // notify msg for live replicas
        if partition.has_live_replicas() {
            notify_msg_for_live_replicas(
                &mut actions.spu_messages,
                MsgType::DELETE,
                "DELETE",
                partition.key(),
                &partition,
                partition.live_replicas(),
            );
        }
        */

        Ok(())
    }

    fn process_lrs_update_from_spu(
        &self,
        lrs_req: UpdateLrsRequest,
        actions: &mut PartitionActions,
    ) {
        debug!("updating lrs for replica: {}", lrs_req.id);
        if self
            .partition_store
            .find_and_do(&lrs_req.id, |part_kv| {
                let mut part_status_kv = part_kv.clone();
                let status = PartitionStatus::new2(
                    lrs_req.leader.clone(),
                    lrs_req.replicas.clone(),
                    PartitionResolution::Online,
                );
                part_status_kv.status.merge(status);
                actions
                    .partitions
                    .push(PartitionWSAction::UpdateStatus(part_status_kv));
            })
            .is_none()
        {
            error!("update lrs faild, no replia: {}", lrs_req.id);
        }
    }

    /// perform election when spu goes offline
    fn force_election_spu_off(&self, offline_spu: SpuKV, actions: &mut PartitionActions) {
        debug!(
            "start election when spu went offline: {}",
            offline_spu.key()
        );
        let offline_leader_spu_id = offline_spu.spec.id;

        let spu_status = self.spu_store.online_status();

        let policy = SimplePolicy::new();

        // go thru each partitions whose leader matches offline spu.
        self.partition_store.visit_values(|partition_kv| {
            // find partition who's leader is same as offline spu
            if partition_kv.spec.leader == offline_leader_spu_id {
                // find suitable leader
                if let Some(candidate_leader) = partition_kv.status.candidate_leader(&spu_status, &policy)
                {
                    debug!(
                        "suitable leader has found: {} leader: {}",
                        partition_kv.key(),
                        candidate_leader
                    );
                    let mut part_kv_change = partition_kv.clone();
                    part_kv_change.spec.leader = candidate_leader;
                    actions
                        .partitions
                        .push(PartitionWSAction::UpdateSpec(part_kv_change));
                // change the
                } else {
                    warn!("no suitable leader has found: {}", partition_kv.key());
                    let mut part_kv_change = partition_kv.clone();
                    part_kv_change.status.resolution = PartitionResolution::LeaderOffline;
                    actions
                        .partitions
                        .push(PartitionWSAction::UpdateStatus(part_kv_change));
                }
            }
        });
    }

    /// perform election when spu become online
    fn force_election_spu_on(&self, online_spu: SpuKV, actions: &mut PartitionActions) {
        debug!("start election spu went online: {}", online_spu.key());
        let online_leader_spu_id = online_spu.spec.id;

        let policy = SimplePolicy::new();
        // go thru each partitions which are not online and try to promote given online spu

        self.partition_store.visit_values(|partition_kv| {
            if partition_kv.status.is_offline() {
                // we only care about partition who is follower since, leader will set partition status when it start up
                if partition_kv.spec.leader != online_leader_spu_id {
                    for replica_status in partition_kv.status.replica_iter() {
                        if replica_status.spu == online_leader_spu_id
                            && policy
                                .potential_leader_score(
                                    &replica_status,
                                    &partition_kv.status.leader,
                                )
                                .is_suitable()
                        {
                            debug!(
                                "suitable leader has found: {} leader: {}",
                                partition_kv.key(),
                                online_leader_spu_id
                            );
                            let mut part_kv_change = partition_kv.clone();
                            part_kv_change.spec.leader = online_leader_spu_id;
                            actions
                                .partitions
                                .push(PartitionWSAction::UpdateSpec(part_kv_change));
                        }
                    }
                }
            }
        });
    }
}

struct SimplePolicy {}

impl SimplePolicy {
    fn new() -> Self {
        SimplePolicy {}
    }
}

impl ElectionPolicy for SimplePolicy {
    fn potential_leader_score(
        &self,
        replica_status: &ReplicaStatus,
        leader: &ReplicaStatus,
    ) -> ElectionScoring {
        let lag = leader.leo - replica_status.leo;
        if lag < 4 {
            ElectionScoring::Score(lag as u16)
        } else {
            ElectionScoring::NotSuitable
        }
    }
}

// -----------------------------------
//  Unit Tests
//      >> utils::init_logger();
//      >> RUST_LOG=sc_server=trace cargo test <test-name>
// -----------------------------------

#[cfg(test)]
pub mod test {
    use utils::actions::Actions;

    use super::PartitionReducer;
    use super::PartitionChangeRequest;
    use super::PartitionWSAction;
    use super::super::PartitionLSChange;

    #[test]
    fn test_process_partition_actions_without_partitions() {
        // utils::init_logger();

        let partition_reducer = PartitionReducer::default();

        let partition_requests: Actions<PartitionLSChange> = vec![
            // action, (topic,replica), (leader,lrs)
            PartitionLSChange::Add((("topic1", 0), vec![1, 2, 3]).into()),
            PartitionLSChange::Add((("topic1", 1), vec![2, 3, 1]).into()),
        ]
        .into();

        // Run Test
        let _actions = partition_reducer
            .process_requests(PartitionChangeRequest::Partition(partition_requests))
            .expect("actions");

        // partitions
        let _expected_partitions: Actions<PartitionWSAction> = vec![
            PartitionWSAction::UpdateStatus((("topic1", 0), vec![1, 2, 3]).into()),
            PartitionWSAction::UpdateStatus((("topic1", 1), vec![2, 3, 1]).into()),
        ]
        .into();

        // assert_eq!(actions.partitions,expected_partitions);

        // leader message queue
        /*
         TODO: Fix this
        let expected_msgs_for_select_spus: SpuNotifyById<ReplicaMsg> = SpuNotifyById::default();
        let mut leader_msgs = gen_leader_msg_vec(vec![
            //action, name, leader, live_replicas
            (TAction::UPDATE, ("topic1", 0), 1, vec![1, 2, 3]),
            (TAction::UPDATE, ("topic1", 1), 2, vec![2, 3, 1]),
        ]);
        expected_msgs_for_select_spus.push(&2, leader_msgs.pop().unwrap());
        expected_msgs_for_select_spus.push(&1, leader_msgs.pop().unwrap());

        assert_eq!(
            msgs_for_spus,
            expected_msgs_for_select_spus
        );
        */
    }

    /*
    #[test]
    fn test_process_partition_actions_with_partitions() {
        // utils::init_logger();

        let partitions = create_partitions(vec![
            // topic, idx, epoch, replicas
            (("topic1", 0), 0, vec![0, 1, 2]),
            (("topic1", 1), 0, vec![2, 3, 1]),
            (("topic2", 0), 0, vec![1, 2, 0]),
        ]);
        let partition_actions = create_partition_actions(&vec![
            // action, topic, idx, (epoch lrs), Some(epoch, lrs)
            (TAction::ADD, "topic1", 0, (5, vec![0, 1, 2]), None),
            (
                TAction::MOD,
                "topic1",
                1,
                (1, vec![2, 3, 1]),
                Some((0, vec![2, 3, 1])),
            ),
            (TAction::DEL, "topic2", 0, (0, vec![1, 2, 0]), None),
        ]);
        let mut ctx = PartitionContext::default().with_partition_actions(partition_actions);

        // Run Test
        let res = process_partition_actions(
            &partitions,
            ctx.partition_actions(),
            ctx.actions_for_kvs()
        );

        // Validate Result
        assert_eq!(res.is_ok(), true);

        // partitions
        let expected_partitions = create_partitions(vec![
            // topic, idx, epoch, replicas
            (("topic1", 0), 5, vec![0, 1, 2]),
            (("topic1", 1), 1, vec![2, 3, 1]),
        ]);
        assert_eq!(partitions, expected_partitions);

        // partition kvs actions
        let expected_partition_actions: Actions<PartitionKvsAction> = Actions::default();
        assert_eq!(ctx.takeover_actions_for_kvs(), expected_partition_actions);

        // leader messages
        let expected_msgs_for_select_spus: SpuNotifyById<ReplicaMsg> = SpuNotifyById::default();
        let mut leader_msgs = gen_leader_msg_vec(vec![
            //action, name, epoch, leader, live_replicas
            (TAction::UPDATE, ("topic1", 0), 5, 0, vec![0, 1, 2]),
            (TAction::DEL, ("topic2", 0), 0, 1, vec![1, 2, 0]),
            (TAction::UPDATE, ("topic1", 1), 1, 2, vec![2, 3, 1]),
        ]);

        expected_msgs_for_select_spus.push(&2, leader_msgs.pop().unwrap());
        expected_msgs_for_select_spus.push(&1, leader_msgs.pop().unwrap());
        expected_msgs_for_select_spus.push(&0, leader_msgs.pop().unwrap());
        assert_eq!(
            ctx.takeover_msgs_for_select_spus(),
            expected_msgs_for_select_spus
        );
    }
    */
}
