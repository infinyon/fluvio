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
