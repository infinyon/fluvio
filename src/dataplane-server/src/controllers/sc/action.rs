use flv_metadata_cluster::partition::ReplicaKey;

#[derive(Debug)]
pub enum SupervisorCommand {
    #[allow(dead_code)]
    ReplicaLeaderTerminated(ReplicaKey),
}
