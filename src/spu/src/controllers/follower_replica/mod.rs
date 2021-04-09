mod state;
mod follower_controller;
mod api_key;
mod peer_api;
pub mod sync;

pub(crate) use self::follower_controller::ReplicaFollowerController;
pub use self::state::FollowersState;
pub use self::state::SharedFollowersState;

use fluvio_controlplane_metadata::partition::Replica;

#[derive(Debug)]
pub enum FollowerReplicaControllerCommand {
    AddReplica(Replica),
    UpdateReplica(Replica),
}
