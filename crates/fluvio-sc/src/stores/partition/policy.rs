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

pub(crate) trait ElectionPolicy {
    /// compute potential leade score against leader
    fn potential_leader_score(
        &self,
        replica_status: &ReplicaStatus,
        leader: &ReplicaStatus,
    ) -> ElectionScoring;
}

pub(crate) struct SimplePolicy {}

impl SimplePolicy {
    pub(crate) fn new() -> Self {
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
