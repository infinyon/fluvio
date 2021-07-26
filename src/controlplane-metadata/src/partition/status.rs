#![allow(clippy::assign_op_pattern)]

//!
//! # Partition Status
//!
//! Partition Status metadata information cached locally.
//!
use std::collections::HashSet;
use std::fmt;
use std::slice::Iter;

use dataplane::core::{Encoder, Decoder};
use dataplane::Offset;
use fluvio_types::SpuId;

use super::ElectionPolicy;
use super::ElectionScoring;

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Decoder, Encoder, Default, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct PartitionStatus {
    pub resolution: PartitionResolution,
    pub leader: ReplicaStatus,
    pub lrs: u32,
    pub replicas: Vec<ReplicaStatus>,
    pub is_being_deleted: bool,
}

impl fmt::Display for PartitionStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?} Leader: {} [", self.resolution, self.leader)?;
        for replica in &self.replicas {
            write!(f, "{},", replica)?;
        }
        write!(f, "]")
    }
}

// -----------------------------------
// Implementation
// -----------------------------------

impl PartitionStatus {
    pub fn leader(leader: impl Into<ReplicaStatus>) -> Self {
        Self::new(leader.into(), vec![])
    }

    pub fn new(leader: impl Into<ReplicaStatus>, replicas: Vec<ReplicaStatus>) -> Self {
        Self {
            resolution: PartitionResolution::default(),
            leader: leader.into(),
            replicas,
            ..Default::default()
        }
    }

    pub fn new2(
        leader: impl Into<ReplicaStatus>,
        replicas: Vec<ReplicaStatus>,
        resolution: PartitionResolution,
    ) -> Self {
        Self {
            resolution,
            leader: leader.into(),
            replicas,
            ..Default::default()
        }
    }

    pub fn is_online(&self) -> bool {
        self.resolution == PartitionResolution::Online
    }

    pub fn is_offline(&self) -> bool {
        self.resolution != PartitionResolution::Online
    }

    pub fn lrs(&self) -> u32 {
        self.lrs
    }

    pub fn replica_iter(&self) -> Iter<ReplicaStatus> {
        self.replicas.iter()
    }

    pub fn live_replicas(&self) -> Vec<i32> {
        self.replicas.iter().map(|lrs| lrs.spu).collect()
    }

    pub fn offline_replicas(&self) -> Vec<i32> {
        vec![]
    }

    pub fn has_live_replicas(&self) -> bool {
        !self.replicas.is_empty()
    }

    /// set to being deleted
    pub fn set_to_delete(mut self) -> Self {
        self.is_being_deleted = true;
        self
    }

    /// Fnd best candidate from online replicas
    /// If there are multiple matches, find with best score (lowest lag)
    pub fn candidate_leader<P>(&self, online: &HashSet<SpuId>, policy: &P) -> Option<SpuId>
    where
        P: ElectionPolicy,
    {
        let mut candiate_spu = None;
        let mut best_score = 0;

        for candidate in &self.replicas {
            // only do for live replicas
            if online.contains(&candidate.spu) {
                if let ElectionScoring::Score(score) =
                    policy.potential_leader_score(&candidate, &self.leader)
                {
                    if candiate_spu.is_some() {
                        if score < best_score {
                            best_score = score;
                            candiate_spu = Some(candidate.spu);
                        }
                    } else {
                        best_score = score;
                        candiate_spu = Some(candidate.spu);
                    }
                }
            }
        }
        candiate_spu
    }

    /// merge status from spu
    /// ignore changes from spu = -1 or offsets = -1
    pub fn merge(&mut self, other: Self) {
        self.resolution = other.resolution;
        if let Some(old) = self.leader.merge(&other.leader) {
            self.replicas.push(old); // move old leader to replicas
        }

        for status in other.replicas {
            if let Some(old_status) = find_status(&mut self.replicas, status.spu) {
                old_status.merge(&status);
            } else {
                self.replicas.push(status);
            }
        }
        // delete any old status for leader in the follower
        let spu = self.leader.spu;
        self.replicas = self
            .replicas
            .iter()
            .filter_map(move |s| {
                if s.spu != spu {
                    Some(s.to_owned())
                } else {
                    None
                }
            })
            .collect();
        self.update_lrs();
    }

    /// recalculate lrs which is count of follower whose leo is same as leader
    fn update_lrs(&mut self) {
        let leader_leo = self.leader.leo;
        self.lrs = self
            .replicas
            .iter()
            .filter(|re| re.leo != -1 && leader_leo == re.leo)
            .count() as u32;
    }
}

/// find status matching it,
fn find_status(status: &mut Vec<ReplicaStatus>, spu: SpuId) -> Option<&'_ mut ReplicaStatus> {
    status.iter_mut().find(|status| status.spu == spu)
}

#[derive(Decoder, Encoder, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PartitionResolution {
    Offline,             // No leader available for serving partition
    Online,              // Partition is running normally, status contains replica info
    LeaderOffline,       // Election has failed, no suitable leader has been founded
    ElectionLeaderFound, // New leader has been selected
}

impl Default for PartitionResolution {
    fn default() -> Self {
        PartitionResolution::Offline
    }
}

#[derive(Decoder, Encoder, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ReplicaStatus {
    pub spu: i32,
    pub hw: i64,
    pub leo: i64,
}

impl fmt::Display for ReplicaStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "spu:{} hw:{} leo: {}", self.spu, self.hw, self.leo)
    }
}

impl Default for ReplicaStatus {
    fn default() -> Self {
        ReplicaStatus {
            spu: -1,
            hw: -1,
            leo: -1,
        }
    }
}

impl ReplicaStatus {
    pub fn new(spu: SpuId, hw: Offset, leo: Offset) -> Self {
        Self { spu, hw, leo }
    }

    /// compute lag score respect to leader
    pub fn leader_lag(&self, leader_status: &Self) -> i64 {
        leader_status.leo - self.leo
    }

    pub fn high_watermark_lag(&self, leader_status: &Self) -> i64 {
        leader_status.hw - self.hw
    }

    /// merge status
    pub fn merge(&mut self, source: &Self) -> Option<Self> {
        // if source spu is -1, we ignore it
        if source.spu == -1 {
            return None;
        }

        // if spu is same, we override otherwise, we copy but return old
        if self.spu == -1 || self.spu == source.spu {
            self.spu = source.spu;

            if source.leo != -1 {
                self.leo = source.leo;
            }

            if source.hw != -1 {
                self.hw = source.hw;
            }
            None
        } else {
            let old = Self::new(self.spu, self.hw, self.leo);

            self.spu = source.spu;

            self.leo = source.leo;
            self.hw = source.hw;

            Some(old)
        }
    }
}

impl From<(SpuId, Offset, Offset)> for ReplicaStatus {
    fn from(val: (SpuId, Offset, Offset)) -> Self {
        let (id, high_watermark, end_offset) = val;
        Self::new(id, high_watermark, end_offset)
    }
}

#[cfg(test)]
mod test {

    use std::collections::HashSet;

    use super::PartitionStatus;
    use super::ReplicaStatus;
    use super::ElectionPolicy;
    use super::ElectionScoring;

    struct SimplePolicy {}

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

    #[test]
    fn test_candidate_spu_no_candidate() {
        let status = PartitionStatus::leader((5000, 0, 0));
        let online_spu = HashSet::new();
        let policy = SimplePolicy {};

        assert!(status.candidate_leader(&online_spu, &policy).is_none());
    }

    #[test]
    fn test_candidate_spu_best() {
        let status = PartitionStatus::new(
            (5000, 100, 110),
            vec![
                (5001, 100, 110).into(), // caught up with leader  (best)
                (5002, 100, 105).into(), // need 5 offset to caught with leaser
            ],
        );
        let mut online_spu = HashSet::new();
        online_spu.insert(5001);
        online_spu.insert(5002);
        let policy = SimplePolicy {};

        assert_eq!(status.candidate_leader(&online_spu, &policy), Some(5001)); // 5001 has least lag
    }

    /// we only care about which has least lag of end offset
    /// even if follower didn't catch up HW
    #[test]
    fn test_candidate_spu_best_conflict() {
        let status = PartitionStatus::new(
            (5000, 100, 110),
            vec![
                (5001, 95, 110).into(),  // caught up with leader  (best)
                (5002, 100, 105).into(), // need 5 offset to caught with leaser
            ],
        );

        let mut online_spu = HashSet::new();
        online_spu.insert(5000);
        online_spu.insert(5001);
        online_spu.insert(5002);
        let policy = SimplePolicy {};

        assert_eq!(status.candidate_leader(&online_spu, &policy), Some(5001)); // 5001 has least lag
    }

    /// check when we don't have any online
    #[test]
    fn test_candidate_spu_no_online() {
        let status = PartitionStatus::new(
            (5000, 100, 110),
            vec![
                (5001, 95, 110).into(),  // caught up with leader  (best)
                (5002, 100, 105).into(), // need 5 offset to caught with leaser
            ],
        );

        let online_spu = HashSet::new();
        let policy = SimplePolicy {};

        assert!(status.candidate_leader(&online_spu, &policy).is_none());
    }

    #[test]
    fn test_merge_initial() {
        let mut target = PartitionStatus::default();
        let source = PartitionStatus::leader((5000, 10, 11));
        target.merge(source);
        assert_eq!(target.leader, (5000, 10, 11).into());
        assert_eq!(target.replicas.len(), 0);

        let source = PartitionStatus::new((5000, 10, 11), vec![(5001, 9, 11).into()]);
        target.merge(source);

        assert_eq!(target.replicas.len(), 1);
        assert_eq!(target.replicas[0], (5001, 9, 11).into());
    }

    #[test]
    fn test_merge_lrs_full() {
        let mut target = PartitionStatus::new(
            (5000, 100, 110),
            vec![(5001, 95, 110).into(), (5002, 100, 105).into()],
        );

        let source = PartitionStatus::new(
            (5000, 120, 120),
            vec![(5002, 110, 120).into(), (5001, -1, -1).into()],
        );

        target.merge(source);

        assert_eq!(target.leader, (5000, 120, 120).into());
        assert_eq!(target.replicas[0], (5001, 95, 110).into());
        assert_eq!(target.replicas[1], (5002, 110, 120).into());
    }

    #[test]
    fn test_merge_lrs_different_leader() {
        let mut target = PartitionStatus::new((5000, 100, 110), vec![(5001, 95, 110).into()]);

        let source = PartitionStatus::new((5001, 120, 120), vec![(5000, -1, -1).into()]);

        target.merge(source);

        assert_eq!(target.leader, (5001, 120, 120).into());
        assert_eq!(target.replicas.len(), 1);
        assert_eq!(target.replicas[0], (5000, 100, 110).into());
    }

    #[test]
    fn test_merge_lrs_case_2() {
        let mut target =
            PartitionStatus::new((5002, 0, 0), vec![(5002, 0, 0).into(), (5001, 0, 0).into()]);

        let source = PartitionStatus::new((5002, 0, 0), vec![(5001, -1, -1).into()]);

        target.merge(source);

        assert_eq!(target.leader, (5002, 0, 0).into());
        assert_eq!(target.replicas.len(), 1);
        assert_eq!(target.replicas[0], (5001, 0, 0).into());
    }
}
