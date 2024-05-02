use fluvio_controlplane_metadata::partition::ReplicaKey;

#[allow(dead_code)]
#[derive(Debug)]
pub enum SupervisorCommand {
    ReplicaLeaderTerminated(ReplicaKey),
}
