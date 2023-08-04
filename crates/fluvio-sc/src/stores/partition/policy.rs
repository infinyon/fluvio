use super::ReplicaStatus;

pub enum ElectionScoring {
    NotSuitable,
    Score(u16), // 0 is perfect
}

impl ElectionScoring {
    pub fn is_suitable(&self) -> bool {
        match self {
            Self::NotSuitable => false,
            Self::Score(_) => true,
        }
    }
}

pub trait ElectionPolicy {
    /// compute potential leade score against leader
    fn potential_leader_score(
        &self,
        replica_status: &ReplicaStatus,
        leader: &ReplicaStatus,
    ) -> ElectionScoring;
}

impl ElectionPolicy for ElectionPolicy {

/// Fnd best candidate from online replicas
    /// If there are multiple matches, find with best score (lowest lag)
    fn candidate_leader<P>(&self, online: &HashSet<SpuId>, policy: &P) -> Option<SpuId>
    where
        P: ElectionPolicy,
    {
        let mut candidate_spu = None;
        let mut best_score = 0;

        for candidate in &self.replicas {
            // only do for live replicas
            if online.contains(&candidate.spu) {
                if let ElectionScoring::Score(score) =
                    policy.potential_leader_score(candidate, &self.leader)
                {
                    if candidate_spu.is_some() {
                        if score < best_score {
                            best_score = score;
                            candidate_spu = Some(candidate.spu);
                        }
                    } else {
                        best_score = score;
                        candidate_spu = Some(candidate.spu);
                    }
                }
            }
        }
        candidate_spu
    }


}

/// find status matching it,
fn find_status(status: &mut [ReplicaStatus], spu: SpuId) -> Option<&'_ mut ReplicaStatus> {
    status.iter_mut().find(|status| status.spu == spu)
}

pub(crate) trait PartitonStatusSC {

}

impl PartitonStatusSC for PartitionStatus {

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

    /// merge status from spu
    /// ignore changes from spu = -1 or offsets = -1
    pub fn merge(&mut self, other: Self) {
        self.resolution = other.resolution;
        self.size = other.size;
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
        self.lsr = self
            .replicas
            .iter()
            .filter(|re| re.leo != -1 && leader_leo == re.leo)
            .count() as u32;
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
